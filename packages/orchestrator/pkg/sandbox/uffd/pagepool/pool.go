package pagepool

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
)

type pageEntry struct {
	once sync.Once
	err  error
}

type PagePool struct {
	buildID  string
	memfdFd  int
	size     int64
	pageSize int64
	mapping  []byte

	pages sync.Map
	mu    sync.RWMutex
	// populated is a simple bitset: one bit per page index.
	// Protected by mu for writes; reads use RLock.
	populated []uint64

	warming  atomic.Bool
	warmDone chan struct{}
}

func NewPagePool(buildID string, totalSize, pageSize int64, hugePages bool) (*PagePool, error) {
	flags := unix.MFD_CLOEXEC
	if hugePages {
		flags |= unix.MFD_HUGETLB | unix.MFD_HUGE_2MB
	}

	name := fmt.Sprintf("e2b-pagepool-%s", buildID)
	if len(name) > 249 {
		name = name[:249]
	}

	fd, err := unix.MemfdCreate(name, flags)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}

	if err := syscall.Ftruncate(fd, totalSize); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("ftruncate memfd to %d: %w", totalSize, err)
	}

	// MAP_NORESERVE so hugepages are allocated lazily on first write, not
	// reserved upfront for the full memfd size.
	mmapFlags := syscall.MAP_SHARED | syscall.MAP_NORESERVE
	if hugePages {
		mmapFlags |= unix.MAP_HUGETLB | unix.MAP_HUGE_2MB
	}

	mapping, err := syscall.Mmap(fd, 0, int(totalSize), syscall.PROT_READ|syscall.PROT_WRITE, mmapFlags)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap memfd: %w", err)
	}

	numPages := (totalSize + pageSize - 1) / pageSize
	bitmapLen := (numPages + 63) / 64

	return &PagePool{
		buildID:   buildID,
		memfdFd:   fd,
		size:      totalSize,
		pageSize:  pageSize,
		mapping:   mapping,
		populated: make([]uint64, bitmapLen),
		warmDone:  make(chan struct{}),
	}, nil
}

func (p *PagePool) MemfdFd() int {
	return p.memfdFd
}

func (p *PagePool) MemfdPath() string {
	return fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), p.memfdFd)
}

func (p *PagePool) Size() int64 {
	return p.size
}

func (p *PagePool) PageSize() int64 {
	return p.pageSize
}

func (p *PagePool) IsPopulated(offset int64) bool {
	idx := uint64(offset / p.pageSize)
	word := idx / 64
	bit := idx % 64

	p.mu.RLock()
	defer p.mu.RUnlock()

	if word >= uint64(len(p.populated)) {
		return false
	}

	return p.populated[word]&(1<<bit) != 0
}

func (p *PagePool) markPopulated(offset int64) {
	idx := uint64(offset / p.pageSize)
	word := idx / 64
	bit := idx % 64

	p.mu.Lock()
	p.populated[word] |= 1 << bit
	p.mu.Unlock()
}

func (p *PagePool) validateOffset(offset int64) error {
	if offset < 0 || offset%p.pageSize != 0 {
		return fmt.Errorf("invalid offset %d: must be non-negative and aligned to pageSize %d", offset, p.pageSize)
	}
	if offset+p.pageSize > int64(len(p.mapping)) {
		return fmt.Errorf("offset %d + pageSize %d exceeds mapping size %d", offset, p.pageSize, len(p.mapping))
	}
	return nil
}

// EnsurePagePopulated populates the page at offset by reading from source.
// Returns true if this call was the first writer.
func (p *PagePool) EnsurePagePopulated(ctx context.Context, offset int64, source block.Slicer) (bool, error) {
	if p.IsPopulated(offset) {
		return false, nil
	}

	if err := p.validateOffset(offset); err != nil {
		return false, err
	}

	entryI, _ := p.pages.LoadOrStore(offset, &pageEntry{})
	entry := entryI.(*pageEntry)

	wasFirst := false
	entry.once.Do(func() {
		data, err := source.Slice(ctx, offset, p.pageSize)
		if err != nil {
			entry.err = fmt.Errorf("slice offset %d: %w", offset, err)
			return
		}

		if int64(len(data)) != p.pageSize {
			entry.err = fmt.Errorf("slice returned %d bytes, expected %d at offset %d", len(data), p.pageSize, offset)
			return
		}

		copy(p.mapping[offset:offset+p.pageSize], data)
		p.markPopulated(offset)
		wasFirst = true
	})

	return wasFirst, entry.err
}

func (p *PagePool) EnsurePagePopulatedDirect(offset int64, data []byte) error {
	if p.IsPopulated(offset) {
		return nil
	}

	if err := p.validateOffset(offset); err != nil {
		return err
	}

	if int64(len(data)) != p.pageSize {
		return fmt.Errorf("data length %d does not match pageSize %d at offset %d", len(data), p.pageSize, offset)
	}

	entryI, _ := p.pages.LoadOrStore(offset, &pageEntry{})
	entry := entryI.(*pageEntry)

	entry.once.Do(func() {
		copy(p.mapping[offset:offset+p.pageSize], data)
		p.markPopulated(offset)
	})

	return nil
}

// StartWarmup eagerly populates the entire memfd so subsequent faults
// resolve as MINOR → UFFDIO_CONTINUE. Idempotent.
func (p *PagePool) StartWarmup(source block.Slicer) {
	if !p.warming.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer close(p.warmDone)
		ctx := context.Background()
		numPages := p.size / p.pageSize

		var wg errgroup.Group
		wg.SetLimit(32)

		var populatedCount atomic.Int64
		var skippedCount atomic.Int64

		for i := int64(0); i < numPages; i++ {
			offset := i * p.pageSize

			wg.Go(func() error {
				if p.IsPopulated(offset) {
					skippedCount.Add(1)
					return nil
				}

				data, err := source.Slice(ctx, offset, p.pageSize)
				if err != nil {
					skippedCount.Add(1)
					return nil
				}

				if err := p.EnsurePagePopulatedDirect(offset, data); err != nil {
					skippedCount.Add(1)
					return nil
				}
				populatedCount.Add(1)
				return nil
			})
		}

		wg.Wait()
		fmt.Fprintf(os.Stderr, "pagepool warmup done: total=%d populated=%d skipped=%d\n", numPages, populatedCount.Load(), skippedCount.Load())
	}()
}

func (p *PagePool) WaitWarmup(ctx context.Context) error {
	select {
	case <-p.warmDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PagePool) Close() error {
	var munmapErr, closeErr error

	if p.mapping != nil {
		munmapErr = syscall.Munmap(p.mapping)
		p.mapping = nil
	}

	if p.memfdFd >= 0 {
		closeErr = syscall.Close(p.memfdFd)
		p.memfdFd = -1
	}

	if munmapErr != nil {
		return fmt.Errorf("munmap: %w", munmapErr)
	}

	if closeErr != nil {
		return fmt.Errorf("close memfd: %w", closeErr)
	}

	return nil
}
