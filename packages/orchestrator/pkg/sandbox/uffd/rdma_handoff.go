package uffd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// RDMAHandoff is a MemoryBackend that performs the standard UFFD socket
// dance with FC, then hands the UFFD fd off to an external rdma-dest agent
// via OnReceived instead of running Userfaultfd.Serve in-process.
type RDMAHandoff struct {
	size       int64
	blockSize  int64
	socketPath string

	// OnReceived takes ownership of uffdFd. It must spawn rdma-dest (which
	// inherits the fd via ExtraFiles), wait for QP_RTS, then return; only
	// then does FC's resume path unblock.
	OnReceived func(ctx context.Context, uffdFd uintptr, mapping *memory.Mapping) error

	lis        *net.UnixListener
	exit       *utils.ErrorOnce
	readyCh    chan struct{}
	readyOnce  sync.Once
	finishOnce sync.Once
}

var _ MemoryBackend = (*RDMAHandoff)(nil)

func NewRDMAHandoff(size, blockSize int64, socketPath string) *RDMAHandoff {
	return &RDMAHandoff{
		size:       size,
		blockSize:  blockSize,
		socketPath: socketPath,
		exit:       utils.NewErrorOnce(),
		readyCh:    make(chan struct{}),
	}
}

// Finish unblocks Exit().Wait() so the sandbox lifecycle goroutine can tear
// down once the migration is complete.
func (h *RDMAHandoff) Finish(err error) {
	h.finishOnce.Do(func() {
		if err != nil {
			h.exit.SetError(err)
		} else {
			h.exit.SetSuccess()
		}
		h.readyOnce.Do(func() { close(h.readyCh) })
	})
}

func (h *RDMAHandoff) Start(ctx context.Context, sandboxID string) error {
	if h.OnReceived == nil {
		return errors.New("RDMAHandoff.OnReceived not configured")
	}
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: h.socketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen %s: %w", h.socketPath, err)
	}
	h.lis = lis
	if err := os.Chmod(h.socketPath, 0o777); err != nil {
		_ = lis.Close()
		return fmt.Errorf("chmod %s: %w", h.socketPath, err)
	}

	go func() {
		err := h.handle(ctx, sandboxID)
		if err != nil {
			h.exit.SetError(fmt.Errorf("rdma handoff: %w", err))
		}
		_ = h.lis.Close()
		h.readyOnce.Do(func() { close(h.readyCh) })
	}()

	return nil
}

func (h *RDMAHandoff) handle(ctx context.Context, _ string) error {
	if err := h.lis.SetDeadline(time.Now().Add(uffdMsgListenerTimeout)); err != nil {
		return fmt.Errorf("set listener deadline: %w", err)
	}

	conn, err := h.lis.Accept()
	if err != nil {
		return fmt.Errorf("accept fc: %w", err)
	}
	uconn := conn.(*net.UnixConn)
	defer uconn.Close()

	regionsBuf := make([]byte, regionMappingsSize)
	fdBuf := make([]byte, syscall.CmsgSpace(fdSize))
	nMsg, nFd, _, _, err := uconn.ReadMsgUnix(regionsBuf, fdBuf)
	if err != nil {
		return fmt.Errorf("read fc msg: %w", err)
	}
	regionsBuf = regionsBuf[:nMsg]

	var regions []memory.Region
	if err := json.Unmarshal(regionsBuf, &regions); err != nil {
		return fmt.Errorf("parse regions json: %w", err)
	}
	mapping := memory.NewMapping(regions)

	cmsgs, err := syscall.ParseSocketControlMessage(fdBuf[:nFd])
	if err != nil {
		return fmt.Errorf("parse cmsgs: %w", err)
	}
	if len(cmsgs) != 1 {
		return fmt.Errorf("expected 1 control message, got %d", len(cmsgs))
	}
	fds, err := syscall.ParseUnixRights(&cmsgs[0])
	if err != nil {
		return fmt.Errorf("parse fds: %w", err)
	}
	if len(fds) != 1 {
		return fmt.Errorf("expected 1 fd, got %d", len(fds))
	}

	if err := h.OnReceived(ctx, uintptr(fds[0]), mapping); err != nil {
		_ = syscall.Close(fds[0])
		return fmt.Errorf("on-received: %w", err)
	}

	h.readyOnce.Do(func() { close(h.readyCh) })
	return nil
}

func (h *RDMAHandoff) Stop() error {
	if h.lis != nil {
		_ = h.lis.Close()
	}
	return nil
}

func (h *RDMAHandoff) Ready() chan struct{} { return h.readyCh }
func (h *RDMAHandoff) Exit() *utils.ErrorOnce { return h.exit }

func (h *RDMAHandoff) Prefault(_ context.Context, _ int64, _ []byte) error {
	return nil
}

func (h *RDMAHandoff) DiffMetadata(ctx context.Context, f *fc.Process) (*header.DiffMetadata, error) {
	diffInfo, err := f.MemoryInfo(ctx, h.blockSize)
	if err != nil {
		return nil, err
	}
	diffInfo.Dirty.AndNot(diffInfo.Empty)
	pages := header.TotalBlocks(h.size, h.blockSize)
	empty := roaring.Flip(diffInfo.Dirty, 0, uint64(pages))
	empty.RemoveRange(uint64(pages), uint64(1)<<32)
	return &header.DiffMetadata{
		Dirty:     diffInfo.Dirty,
		Empty:     empty,
		BlockSize: h.blockSize,
	}, nil
}

func (h *RDMAHandoff) PrefetchData(_ context.Context) (block.PrefetchData, error) {
	return block.PrefetchData{BlockSize: h.blockSize}, nil
}

func (h *RDMAHandoff) LastFatalReason() string {
	return ""
}
