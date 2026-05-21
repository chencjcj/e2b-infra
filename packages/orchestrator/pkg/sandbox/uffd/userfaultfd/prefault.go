package userfaultfd

import (
	"context"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
)

// Prefault populates the shared memfd (no PTE install) so the first read fires
// as MINOR and resolves via CONTINUE. Falls back to UFFDIO_COPY when pagePool is nil.
func (u *Userfaultfd) Prefault(ctx context.Context, offset int64, data []byte) error {
	ctx, span := tracer.Start(ctx, "prefault page")
	defer span.End()

	addr, err := u.ma.GetHostVirtAddr(offset)
	if err != nil {
		return fmt.Errorf("failed to get host virtual address: %w", err)
	}

	if len(data) != int(u.pageSize) {
		return fmt.Errorf("data length (%d) does not match pagesize (%d)", len(data), u.pageSize)
	}

	if u.pagePool != nil {
		return u.pagePool.EnsurePagePopulatedDirect(offset, data)
	}

	return u.faultPage(ctx, addr, offset, directDataSource{data, int64(u.pageSize)}, nil, block.Prefetch)
}

type directDataSource struct {
	data     []byte
	pagesize int64
}

func (d directDataSource) Slice(_ context.Context, _, _ int64) ([]byte, error) {
	return d.data, nil
}

func (d directDataSource) BlockSize() int64 {
	return d.pagesize
}
