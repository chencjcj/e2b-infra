package userfaultfd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/fdexit"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/pagepool"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd")

const maxRequestsInProgress = 4096

const (
	// sliceMaxRetries is the number of times to retry source.Slice() after the initial attempt.
	// Total attempts = sliceMaxRetries + 1.
	sliceMaxRetries = 3
	// sliceRetryBaseDelay is the initial backoff delay before the first retry.
	// Subsequent retries double the delay (exponential backoff), capped at sliceRetryMaxDelay.
	sliceRetryBaseDelay = 50 * time.Millisecond
	// sliceRetryMaxDelay is the maximum backoff delay between retries.
	sliceRetryMaxDelay = 500 * time.Millisecond
)

var ErrUnexpectedEventType = errors.New("unexpected event type")

// hasEvent checks if a specific poll event flag is set in revents.
func hasEvent(revents, event int16) bool {
	return revents&event != 0
}

type Userfaultfd struct {
	fd Fd

	src         block.Slicer
	ma          *memory.Mapping
	pageSize    uintptr
	pageTracker *pageTracker

	// We use the settleRequests to guard the pageTracker so we can access a consistent state of the pageTracker after the requests are finished.
	settleRequests sync.RWMutex

	prefetchTracker *block.PrefetchTracker

	wg errgroup.Group

	// defaultCopyMode overrides the UFFDIO_COPY mode for all faults when non-zero.
	defaultCopyMode CULong

	// When non-nil, MINOR faults resolve via UFFDIO_CONTINUE.
	pagePool *pagepool.PagePool

	totalMinor        atomic.Uint64
	totalMissingRead  atomic.Uint64
	totalMissingWrite atomic.Uint64

	// Enabled via UFFD_FAULT_TRACE_DIR env var.
	faultTrace *faultTracer

	logger logger.Logger
}

const (
	faultTypeMinorRead    = 0
	faultTypeMinorWrite   = 1
	faultTypeMissingRead  = 2
	faultTypeMissingWrite = 3
)

// Format: "<ns_since_start> <offset> <type>" per line.
type faultTracer struct {
	mu      sync.Mutex
	w       *bufio.Writer
	f       *os.File
	startNs int64
}

func newFaultTracer(path string) (*faultTracer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &faultTracer{
		w:       bufio.NewWriterSize(f, 64*1024),
		f:       f,
		startNs: time.Now().UnixNano(),
	}, nil
}

func (t *faultTracer) Record(offset int64, faultType int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delta := time.Now().UnixNano() - t.startNs
	fmt.Fprintf(t.w, "%d %d %d\n", delta, offset, faultType)
	t.w.Flush()
}

func (t *faultTracer) RecordRaw(offset int64, rawFlags uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delta := time.Now().UnixNano() - t.startNs
	fmt.Fprintf(t.w, "R %d %d 0x%x\n", delta, offset, rawFlags)
	t.w.Flush()
}

func (t *faultTracer) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.w.Flush()
	return t.f.Close()
}

func NewUserfaultfdFromFd(fd uintptr, src block.Slicer, m *memory.Mapping, logger logger.Logger, pagePool *pagepool.PagePool) (*Userfaultfd, error) {
	blockSize := src.BlockSize()

	for _, region := range m.Regions {
		if region.PageSize != uintptr(blockSize) {
			return nil, fmt.Errorf("block size mismatch: %d != %d for region %d", region.PageSize, blockSize, region.BaseHostVirtAddr)
		}
	}

	u := &Userfaultfd{
		fd:              Fd(fd),
		src:             src,
		pageSize:        uintptr(blockSize),
		pageTracker:     newPageTracker(uintptr(blockSize)),
		prefetchTracker: block.NewPrefetchTracker(blockSize),
		ma:              m,
		pagePool:        pagePool,
		logger:          logger,
	}

	if dir := os.Getenv("UFFD_FAULT_TRACE_DIR"); dir != "" {
		path := fmt.Sprintf("%s/uffd-trace-%d-%d.log", dir, os.Getpid(), time.Now().UnixNano())
		tracer, err := newFaultTracer(path)
		if err != nil {
			logger.Warn(context.Background(), "failed to create fault tracer", zap.String("path", path), zap.Error(err))
		} else {
			u.faultTrace = tracer
			logger.Info(context.Background(), "UFFD fault trace enabled", zap.String("path", path))
		}
	}

	// By default this was unlimited.
	// Now that we don't skip previously faulted pages we add at least some boundaries to the concurrency.
	// Also, in some brief tests, adding a limit actually improved the handling at high concurrency.
	u.wg.SetLimit(maxRequestsInProgress)

	return u, nil
}

func (u *Userfaultfd) Close() error {
	if u.faultTrace != nil {
		if err := u.faultTrace.Close(); err != nil {
			u.logger.Warn(context.Background(), "failed to close fault trace", zap.Error(err))
		}
	}
	return u.fd.close()
}

func (u *Userfaultfd) Serve(
	ctx context.Context,
	fdExit *fdexit.FdExit,
) error {
	pollFds := []unix.PollFd{
		{Fd: int32(u.fd), Events: unix.POLLIN},
		{Fd: fdExit.Reader(), Events: unix.POLLIN},
	}

	eagainCounter := newCounterReporter(u.logger, "uffd: eagain with no pagefaults (accumulated)")
	defer eagainCounter.Close(ctx)

	noDataCounter := newCounterReporter(u.logger, "uffd: no data in fd (accumulated)")
	defer noDataCounter.Close(ctx)

	exitFdErrorCounter := newCounterReporter(u.logger, "uffd: exit fd poll errors (accumulated)")
	defer exitFdErrorCounter.Close(ctx)

	uffdErrorCounter := newCounterReporter(u.logger, "uffd: uffd fd poll errors (accumulated)")
	defer uffdErrorCounter.Close(ctx)

	pollErrorEvents := map[int16]string{
		unix.POLLHUP:  "POLLHUP",
		unix.POLLERR:  "POLLERR",
		unix.POLLNVAL: "POLLNVAL",
	}

	for {
		if _, err := unix.Poll(
			pollFds,
			-1,
		); err != nil {
			if err == unix.EINTR {
				u.logger.Debug(ctx, "uffd: interrupted polling, going back to polling")

				continue
			}

			if err == unix.EAGAIN {
				u.logger.Debug(ctx, "uffd: eagain during polling, going back to polling")

				continue
			}

			u.logger.Error(ctx, "UFFD serve polling error", zap.Error(err))

			return fmt.Errorf("failed polling: %w", err)
		}

		exitFd := pollFds[1]
		if hasEvent(exitFd.Revents, unix.POLLIN) {
			errMsg := u.wg.Wait()
			if errMsg != nil {
				u.logger.Warn(ctx, "UFFD fd exit error while waiting for goroutines to finish", zap.Error(errMsg))

				return fmt.Errorf("failed to handle uffd: %w", errMsg)
			}

			return nil
		}

		// Track exit fd error events
		for event, name := range pollErrorEvents {
			if hasEvent(exitFd.Revents, event) {
				exitFdErrorCounter.Increase(name)
			}
		}

		uffdFd := pollFds[0]

		// Track uffd error events
		for event, name := range pollErrorEvents {
			if hasEvent(uffdFd.Revents, event) {
				uffdErrorCounter.Increase(name)
			}
		}

		if !hasEvent(uffdFd.Revents, unix.POLLIN) {
			// Uffd is not ready for reading as there is nothing to read on the fd.
			// https://github.com/firecracker-microvm/firecracker/issues/5056
			// https://elixir.bootlin.com/linux/v6.8.12/source/fs/userfaultfd.c#L1149
			// TODO: Check for all the errors
			// - https://docs.kernel.org/admin-guide/mm/userfaultfd.html
			// - https://elixir.bootlin.com/linux/v6.8.12/source/fs/userfaultfd.c
			// - https://man7.org/linux/man-pages/man2/userfaultfd.2.html
			// It might be possible to just check for data != 0 in the syscall.Read loop
			// but I don't feel confident about doing that.
			noDataCounter.Increase("POLLIN")

			continue
		}

		buf := make([]byte, unsafe.Sizeof(UffdMsg{}))

		var pagefaults []*UffdPagefault
		for {
			_, err := syscall.Read(int(u.fd), buf)
			if err == syscall.EINTR {
				u.logger.Debug(ctx, "uffd: interrupted read, reading again")

				continue
			}

			if err == syscall.EAGAIN {
				break
			}

			if err != nil {
				u.logger.Error(ctx, "uffd: read error", zap.Error(err))

				return fmt.Errorf("failed to read: %w", err)
			}

			msg := *(*UffdMsg)(unsafe.Pointer(&buf[0]))

			if msgEvent := getMsgEvent(&msg); msgEvent != UFFD_EVENT_PAGEFAULT {
				u.logger.Error(ctx, "UFFD serve unexpected event type", zap.Any("event_type", msgEvent))

				return ErrUnexpectedEventType
			}

			arg := getMsgArg(&msg)
			pagefault := *(*UffdPagefault)(unsafe.Pointer(&arg[0]))
			pagefaults = append(pagefaults, &pagefault)
		}

		if len(pagefaults) == 0 {
			eagainCounter.Increase("EMPTY_DRAIN")

			continue
		}

		eagainCounter.Log(ctx)
		noDataCounter.Log(ctx)

		var minorCount, missingReadCount, missingWriteCount int
		for _, pagefault := range pagefaults {
			flags := pagefault.flags

			addr := getPagefaultAddress(pagefault)

			offset, err := u.ma.GetOffset(addr)
			if err != nil {
				u.logger.Error(ctx, "UFFD serve get mapping error", zap.Error(err))

				return fmt.Errorf("failed to map: %w", err)
			}

			if u.faultTrace != nil {
				u.faultTrace.RecordRaw(offset, uint64(flags))
			}

			// MINOR must be checked before WRITE: a MINOR+WRITE fault still
			// needs UFFDIO_CONTINUE, not UFFDIO_COPY.
			if flags&UFFD_PAGEFAULT_FLAG_MINOR != 0 && u.pagePool != nil {
				minorCount++
				accessType := block.Read
				traceType := faultTypeMinorRead
				if flags&UFFD_PAGEFAULT_FLAG_WRITE != 0 {
					accessType = block.Write
					traceType = faultTypeMinorWrite
				}
				if u.faultTrace != nil {
					u.faultTrace.Record(offset, traceType)
				}

				u.wg.Go(func() error {
					return u.faultPageContinue(ctx, addr, offset, u.src, fdExit.SignalExit, accessType)
				})

				continue
			}

			if flags&UFFD_PAGEFAULT_FLAG_WRITE != 0 {
				missingWriteCount++
				if u.faultTrace != nil {
					u.faultTrace.Record(offset, faultTypeMissingWrite)
				}
				u.wg.Go(func() error {
					return u.faultPage(ctx, addr, offset, u.src, fdExit.SignalExit, block.Write)
				})

				continue
			}

			if flags == 0 {
				missingReadCount++
				if u.faultTrace != nil {
					u.faultTrace.Record(offset, faultTypeMissingRead)
				}
				u.wg.Go(func() error {
					return u.faultPage(ctx, addr, offset, u.src, fdExit.SignalExit, block.Read)
				})

				continue
			}

			u.logger.Warn(ctx, "UFFD unexpected fault flags", zap.Uint64("flags", uint64(flags)), zap.Uintptr("addr", addr))
			return fmt.Errorf("unexpected event type: %d, closing uffd", flags)
		}

		if minorCount > 0 || missingReadCount > 0 || missingWriteCount > 0 {
			u.totalMinor.Add(uint64(minorCount))
			u.totalMissingRead.Add(uint64(missingReadCount))
			u.totalMissingWrite.Add(uint64(missingWriteCount))

			u.logger.Debug(ctx, "uffd fault batch",
				zap.Int("minor", minorCount),
				zap.Int("missing_read", missingReadCount),
				zap.Int("missing_write", missingWriteCount),
				zap.Uint64("total_minor", u.totalMinor.Load()),
				zap.Uint64("total_missing_read", u.totalMissingRead.Load()),
				zap.Uint64("total_missing_write", u.totalMissingWrite.Load()),
			)
		}
	}
}

func (u *Userfaultfd) FaultCounts() (minor, missingRead, missingWrite uint64) {
	return u.totalMinor.Load(), u.totalMissingRead.Load(), u.totalMissingWrite.Load()
}

func (u *Userfaultfd) PrefetchData() block.PrefetchData {
	// This will be at worst cancelled when the uffd is closed.
	u.settleRequests.Lock()
	// The locking here would work even without using defer (just lock-then-unlock the mutex), but at this point let's make it lock to the clone,
	// so it is consistent even if there is a another uffd call after.
	defer u.settleRequests.Unlock()

	return u.prefetchTracker.PrefetchData()
}

func (u *Userfaultfd) faultPage(
	ctx context.Context,
	addr uintptr,
	offset int64,
	source block.Slicer,
	onFailure func() error,
	accessType block.AccessType,
) error {
	span := trace.SpanFromContext(ctx)

	// The RLock must be called inside the goroutine to ensure RUnlock runs via defer,
	// even if the errgroup is cancelled or the goroutine returns early.
	// This guards against races between marking the page faulted / prefetched
	// and another caller observing the pageTracker or prefetchTracker.
	u.settleRequests.RLock()
	defer u.settleRequests.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			u.logger.Error(ctx, "UFFD serve panic", zap.Any("pagesize", u.pageSize), zap.Any("panic", r))
		}
	}()

	var b []byte
	var dataErr error
	var attempt int

retryLoop:
	for attempt = range sliceMaxRetries + 1 {
		b, dataErr = source.Slice(ctx, offset, int64(u.pageSize))
		if dataErr == nil {
			break
		}

		if attempt >= sliceMaxRetries || ctx.Err() != nil {
			break
		}

		u.logger.Warn(ctx, "UFFD serve slice error, retrying",
			zap.Int("attempt", attempt+1),
			zap.Int("max_attempts", sliceMaxRetries+1),
			zap.Error(dataErr),
		)

		delay := min(sliceRetryBaseDelay<<attempt, sliceRetryMaxDelay)
		jitter := time.Duration(rand.Int63n(int64(delay) / 2))

		backoff := time.NewTimer(delay + jitter)

		select {
		case <-ctx.Done():
			backoff.Stop()

			dataErr = errors.Join(dataErr, ctx.Err())

			break retryLoop
		case <-backoff.C:
		}
	}

	if dataErr != nil {
		signalErr := safeInvoke(onFailure)

		joinedErr := errors.Join(dataErr, signalErr)

		span.RecordError(joinedErr)
		u.logger.Error(ctx, "UFFD serve data fetch error after retries",
			zap.Int("attempts", attempt+1),
			zap.Error(joinedErr),
		)

		return fmt.Errorf("failed to read from source after %d attempts: %w", attempt+1, joinedErr)
	}

	// READ-only path: populate memfd then WAKE — the retry fires as MINOR and
	// faultPageContinue installs a shared PTE. WRITE/Prefetch fall through to
	// UFFDIO_COPY because the page will be COW'd into a private anon page
	// anyway, so populating memfd would just waste a hugepage.
	if u.pagePool != nil && accessType == block.Read {
		if populateErr := u.pagePool.EnsurePagePopulatedDirect(offset, b); populateErr != nil {
			signalErr := safeInvoke(onFailure)
			joinedErr := errors.Join(populateErr, signalErr)
			span.RecordError(joinedErr)
			u.logger.Error(ctx, "UFFD serve populate memfd error", zap.Error(joinedErr))
			return fmt.Errorf("failed to populate memfd at offset %d: %w", offset, joinedErr)
		}

		wakeErr := u.fd.wake(addr, u.pageSize)
		if errors.Is(wakeErr, unix.ESRCH) {
			span.SetAttributes(attribute.Bool("uffd.process_exited", true))
			u.logger.Debug(ctx, "UFFD serve wake: process no longer exists", zap.Error(wakeErr))
			return nil
		}
		if wakeErr != nil {
			signalErr := safeInvoke(onFailure)
			joinedErr := errors.Join(wakeErr, signalErr)
			span.RecordError(joinedErr)
			u.logger.Error(ctx, "UFFD serve uffdio wake error", zap.Error(joinedErr))
			return fmt.Errorf("failed uffdio wake: %w", joinedErr)
		}

		// State tracking happens in faultPageContinue on the MINOR retry.
		return nil
	}

	copyMode := u.defaultCopyMode

	// Performing copy() on UFFD clears the WP bit unless we explicitly tell
	// it not to. We do that for faults caused by a read access. Write accesses
	// would anyways cause clear the write-protection bit.
	if accessType != block.Write {
		copyMode |= UFFDIO_COPY_MODE_WP
	}

	copyErr := u.fd.copy(addr, u.pageSize, b, copyMode)

	if errors.Is(copyErr, unix.EEXIST) {
		// Page is already mapped
		span.SetAttributes(attribute.Bool("uffd.already_mapped", true))

		return nil
	}

	if errors.Is(copyErr, unix.ESRCH) {
		// The faulting thread/process no longer exists — it exited or was killed
		// while the page fetch was in flight. This is expected during sandbox
		// teardown; treat it as benign.
		span.SetAttributes(attribute.Bool("uffd.process_exited", true))
		u.logger.Debug(ctx, "UFFD serve copy error: process no longer exists", zap.Error(copyErr))

		return nil
	}

	if copyErr != nil {
		signalErr := safeInvoke(onFailure)

		joinedErr := errors.Join(copyErr, signalErr)

		span.RecordError(joinedErr)
		u.logger.Error(ctx, "UFFD serve uffdio copy error", zap.Error(joinedErr))

		return fmt.Errorf("failed uffdio copy: %w", joinedErr)
	}

	u.pageTracker.setState(addr, addr+u.pageSize, faulted)
	u.prefetchTracker.Add(offset, accessType)

	return nil
}

func (u *Userfaultfd) faultPageContinue(
	ctx context.Context,
	addr uintptr,
	offset int64,
	source block.Slicer,
	onFailure func() error,
	accessType block.AccessType,
) error {
	span := trace.SpanFromContext(ctx)

	u.settleRequests.RLock()
	defer u.settleRequests.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			u.logger.Error(ctx, "UFFD serve panic in faultPageContinue", zap.Any("pagesize", u.pageSize), zap.Any("panic", r))
		}
	}()

	_, populateErr := u.pagePool.EnsurePagePopulated(ctx, offset, source)
	if populateErr != nil {
		signalErr := safeInvoke(onFailure)
		joinedErr := errors.Join(populateErr, signalErr)

		span.RecordError(joinedErr)
		u.logger.Error(ctx, "UFFD serve page pool populate error", zap.Error(joinedErr))

		return fmt.Errorf("failed to populate page pool at offset %d: %w", offset, joinedErr)
	}

	continueMode := CULong(0)
	if accessType != block.Write {
		continueMode |= UFFDIO_CONTINUE_MODE_WP
	}

	contErr := u.fd.continueMapping(addr, u.pageSize, continueMode)
	if errors.Is(contErr, unix.EEXIST) {
		span.SetAttributes(attribute.Bool("uffd.already_mapped", true))
		return nil
	}

	if errors.Is(contErr, unix.ESRCH) {
		span.SetAttributes(attribute.Bool("uffd.process_exited", true))
		u.logger.Debug(ctx, "UFFDIO_CONTINUE: process no longer exists", zap.Error(contErr))
		return nil
	}

	if contErr != nil {
		signalErr := safeInvoke(onFailure)
		joinedErr := errors.Join(contErr, signalErr)

		span.RecordError(joinedErr)
		u.logger.Error(ctx, "UFFD serve uffdio continue error", zap.Error(joinedErr))

		return fmt.Errorf("failed uffdio continue: %w", joinedErr)
	}

	u.pageTracker.setState(addr, addr+u.pageSize, faulted)
	u.prefetchTracker.Add(offset, accessType)

	return nil
}
