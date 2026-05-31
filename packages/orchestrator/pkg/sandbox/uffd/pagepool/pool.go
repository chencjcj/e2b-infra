package pagepool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
)

// ErrPoolPressureHigh: caller should fall back to UFFDIO_COPY private path
// because copy() into the MAP_NORESERVE mapping risks SIGBUS when pool is full.
var ErrPoolPressureHigh = errors.New("hugepage pool pressure too high; populate deferred")

// PoolUsageFn returns used/total ∈ [0, 1]. Must be cheap (sub-ms).
type PoolUsageFn func() (float64, error)

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
	// populated is a bitset, one bit per page index, guarded by mu.
	populated []uint64

	closeOnce sync.Once

	poolUsageFn  PoolUsageFn
	pressureFrac float64
	skippedCount atomic.Uint64
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

	// MAP_NORESERVE: lazy alloc — pool only takes capacity for pages actually
	// populated by sandbox READ-MISSING faults. SIGBUS risk on exhaustion is
	// gated by SetPressureProbe.
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
	}, nil
}

// SetPressureProbe gates populate() on pool watermark. frac=0 disables.
func (p *PagePool) SetPressureProbe(usageFn PoolUsageFn, frac float64) {
	p.poolUsageFn = usageFn
	p.pressureFrac = frac
}

func (p *PagePool) SkippedPopulateCount() uint64 {
	return p.skippedCount.Load()
}

func (p *PagePool) shouldDeferPopulate() bool {
	if p.poolUsageFn == nil || p.pressureFrac <= 0 {
		return false
	}
	usage, err := p.poolUsageFn()
	if err != nil {
		return false
	}
	return usage >= p.pressureFrac
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

// EnsurePagePopulated populates offset from source. wasFirst=true on first writer.
// Returns ErrPoolPressureHigh when probe blocks the write.
func (p *PagePool) EnsurePagePopulated(ctx context.Context, offset int64, source block.Slicer) (bool, error) {
	if p.IsPopulated(offset) {
		return false, nil
	}

	if err := p.validateOffset(offset); err != nil {
		return false, err
	}

	if p.shouldDeferPopulate() {
		p.skippedCount.Add(1)
		return false, ErrPoolPressureHigh
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

// EnsurePagePopulatedDirect populates offset from data.
// Returns ErrPoolPressureHigh when probe blocks the write.
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

	if p.shouldDeferPopulate() {
		p.skippedCount.Add(1)
		return ErrPoolPressureHigh
	}

	entryI, _ := p.pages.LoadOrStore(offset, &pageEntry{})
	entry := entryI.(*pageEntry)

	entry.once.Do(func() {
		copy(p.mapping[offset:offset+p.pageSize], data)
		p.markPopulated(offset)
	})

	return nil
}

// Close releases the mapping. Idempotent.
func (p *PagePool) Close() error {
	var err error
	p.closeOnce.Do(func() {
		var munmapErr, closeErr error
		if p.mapping != nil {
			munmapErr = syscall.Munmap(p.mapping)
			p.mapping = nil
		}
		if p.memfdFd >= 0 {
			closeErr = syscall.Close(p.memfdFd)
			p.memfdFd = -1
		}

		switch {
		case munmapErr != nil:
			err = fmt.Errorf("munmap: %w", munmapErr)
		case closeErr != nil:
			err = fmt.Errorf("close memfd: %w", closeErr)
		}
	})
	return err
}
