package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/apiclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/rdma"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/pagepool"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

// buildMigrationMemfdFromFile creates a hugetlbfs-backed memfd of the given
// size, ftruncates it, and copies the contents of memfilePath into it via
// MAP_SHARED + read syscalls. The returned *os.File owns the memfd; closing
// it will release the underlying hugepages.
func buildMigrationMemfdFromFile(memfilePath string, size int64) (*os.File, error) {
	flags := unix.MFD_CLOEXEC | unix.MFD_HUGETLB | unix.MFD_HUGE_2MB
	fd, err := unix.MemfdCreate("e2b-migrate-mem", flags)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	if err := unix.Ftruncate(fd, size); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("ftruncate %d: %w", size, err)
	}
	mmapFlags := unix.MAP_SHARED | unix.MAP_NORESERVE | unix.MAP_HUGETLB | unix.MAP_HUGE_2MB
	buf, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, mmapFlags)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("mmap migration memfd: %w", err)
	}
	defer unix.Munmap(buf)

	src, err := os.Open(memfilePath)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open memfile %s: %w", memfilePath, err)
	}
	defer src.Close()

	const chunk = 64 * 1024 * 1024
	off := int64(0)
	for off < size {
		n := int64(chunk)
		if size-off < n {
			n = size - off
		}
		if _, err := src.ReadAt(buf[off:off+n], off); err != nil {
			_ = unix.Close(fd)
			return nil, fmt.Errorf("read memfile @%d: %w", off, err)
		}
		off += n
	}
	return os.NewFile(uintptr(fd), "e2b-migrate-mem"), nil
}

// preFillMemfdFromTemplate eagerly fills every page in the page-pool memfd's
// pagecache from the sandbox's template memfile. The orchestrator already
// does this lazily on FC fault; doing it eagerly here means FC's subsequent
// CreateSnapshot dump won't trigger MISSING faults — every read hits memfd
// pagecache and resolves as MINOR (cheap UFFDIO_CONTINUE).
//
// Pages already in pagecache (covered by either earlier on-demand faults or
// by an earlier preFill on a prior migration) are skipped via PagePool's
// IsPopulated check.
//
// Parallelism: 8 worker goroutines, each handles a page-strided slice of the
// total range. Slice reads are mostly local (template-cache mmap), so we're
// memcpy-bound; 8 workers saturate memory bandwidth without contending on
// the same offsets.
func (s *Server) preFillMemfdFromTemplate(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	pool *pagepool.PagePool,
) error {
	t0 := time.Now()
	memfile, err := sbx.Template.Memfile(ctx)
	if err != nil {
		return fmt.Errorf("get template memfile: %w", err)
	}

	totalSize := pool.Size()
	pageSize := pool.PageSize()
	numPages := (totalSize + pageSize - 1) / pageSize

	const workers = 8
	type work struct{ start, end int64 }
	jobs := make(chan work, workers)
	errsCh := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range jobs {
				for off := w.start; off < w.end; off += pageSize {
					if _, err := pool.EnsurePagePopulated(ctx, off, memfile); err != nil {
						errsCh <- fmt.Errorf("populate offset %d: %w", off, err)
						return
					}
				}
			}
		}()
	}

	// Strided ranges: each worker handles a contiguous chunk.
	chunk := numPages / workers
	if chunk < 1 {
		chunk = 1
	}
	for i := int64(0); i < numPages; i += chunk {
		end := i + chunk
		if end > numPages {
			end = numPages
		}
		jobs <- work{start: i * pageSize, end: end * pageSize}
	}
	close(jobs)
	wg.Wait()
	close(errsCh)

	for e := range errsCh {
		if e != nil {
			return e
		}
	}

	logger.L().Info(ctx, "preFillMemfdFromTemplate: done",
		zap.String("sandbox_id", sbx.Runtime.SandboxID),
		zap.Int64("total_size", totalSize),
		zap.Duration("elapsed", time.Since(t0)))
	return nil
}

type migrationSession struct {
	sandboxID    string
	sbx          *sandbox.Sandbox
	source       *rdma.Source
	memfdFile    *os.File
	snapfilePath string
	memfilePath  string

	doneOnce sync.Once
	done     chan struct{}
	err      error
}

func (s *Server) registerMigration(sess *migrationSession) error {
	s.migrationsMu.Lock()
	defer s.migrationsMu.Unlock()
	if _, exists := s.migrations[sess.sandboxID]; exists {
		return fmt.Errorf("migration already in progress for %s", sess.sandboxID)
	}
	s.migrations[sess.sandboxID] = sess
	return nil
}

func (s *Server) lookupMigration(sandboxID string) *migrationSession {
	s.migrationsMu.Lock()
	defer s.migrationsMu.Unlock()
	return s.migrations[sandboxID]
}

func (s *Server) clearMigration(sandboxID string) {
	s.migrationsMu.Lock()
	defer s.migrationsMu.Unlock()
	delete(s.migrations, sandboxID)
}

func (s *Server) PrepareMigrationSource(
	ctx context.Context,
	in *orchestrator.MigrationPrepareSourceRequest,
) (*orchestrator.MigrationPrepareSourceResponse, error) {
	sandboxID := in.GetSandboxId()
	logger.L().Info(ctx, "PrepareMigrationSource: enter", zap.String("sandbox_id", sandboxID))
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id required")
	}
	if s.rdmaConfig.SourceBinary == "" {
		return nil, status.Error(codes.FailedPrecondition, "RDMA migration not configured (RDMA_SOURCE_BIN unset)")
	}

	sbx, ok := s.sandboxFactory.Sandboxes.Get(sandboxID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", sandboxID)
	}

	pool := s.sandboxFactory.GetPagePool(sbx.Runtime.BuildID)
	if pool == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"sandbox %s has no page pool — RDMA migration requires hugepage shared memory",
			sandboxID)
	}
	logger.L().Info(ctx, "PrepareMigrationSource: page pool resolved",
		zap.String("sandbox_id", sandboxID),
		zap.Int("memfd_fd", pool.MemfdFd()),
		zap.Int64("size", pool.Size()))

	if _, err := sbx.Pid(); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox not ready: %v", err)
	}

	sess := &migrationSession{
		sandboxID: sandboxID,
		sbx:       sbx,
		done:      make(chan struct{}),
	}
	if err := s.registerMigration(sess); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}

	cleanupOnError := func(err error) (*orchestrator.MigrationPrepareSourceResponse, error) {
		s.rollbackMigration(ctx, sess)
		return nil, err
	}

	logger.L().Info(ctx, "PrepareMigrationSource: pausing FC", zap.String("sandbox_id", sandboxID))
	pauseStart := time.Now()
	pauseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := sbx.PauseFC(pauseCtx); err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "pause FC: %v", err))
	}
	logger.L().Info(ctx, "PrepareMigrationSource: FC paused",
		zap.String("sandbox_id", sandboxID),
		zap.Duration("elapsed", time.Since(pauseStart)))

	// /mnt/snapshot-cache is a dedicated tmpfs (RAM-backed) for snapshot/migration
	// scratch. Writing the 2GB memfile here instead of disk turns ~9s into ~1s.
	// Falls back to /dev/shm, then to the orchestrator base dir.
	snapTmpDir := "/mnt/snapshot-cache/e2b-migrate"
	if err := os.MkdirAll(snapTmpDir, 0o755); err != nil {
		snapTmpDir = "/dev/shm/e2b-migrate"
		if err := os.MkdirAll(snapTmpDir, 0o755); err != nil {
			snapTmpDir = filepath.Join(s.config.OrchestratorBaseDir, "tmp")
			if err := os.MkdirAll(snapTmpDir, 0o755); err != nil {
				return cleanupOnError(status.Errorf(codes.Internal, "mkdir snap tmp: %v", err))
			}
		}
	}
	snapTmp, err := os.CreateTemp(snapTmpDir, "migrate-snap-*.snap")
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "create snap tmp: %v", err))
	}
	snapPath := snapTmp.Name()
	_ = snapTmp.Close()
	sess.snapfilePath = snapPath

	memPath := snapPath + ".mem"
	sess.memfilePath = memPath

	// Pre-populate memfd page cache BEFORE FC dumps memory.
	//
	// Without this: FC's CreateSnapshot iterates 2 GB of guest memory; for
	// every page not yet in memfd's pagecache (template pages FC never read),
	// FC's read triggers a UFFD MISSING fault → orch handler reads template
	// from build.File → UFFDIO_COPY/wake. ~3 ms per round-trip × 1024 pages
	// = ~3 s on the FC dump alone.
	//
	// With this: orch eagerly fills memfd's pagecache from the template
	// memfile via the page pool's MAP_SHARED mapping (no UFFD round-trip,
	// just memcpy). When FC then dumps, every page is in pagecache so it
	// MINOR-faults instead of MISSING-faulting — UFFDIO_CONTINUE is much
	// cheaper than the build.File read path. Net win: ~2 s on the dump.
	if err := s.preFillMemfdFromTemplate(ctx, sbx, pool); err != nil {
		// Pre-population is an optimization; fall through to slow path on
		// error so migration still works.
		logger.L().Warn(ctx, "PrepareMigrationSource: pre-populate failed; falling through to slow dump path",
			zap.String("sandbox_id", sandboxID), zap.Error(err))
	}

	logger.L().Info(ctx, "PrepareMigrationSource: creating FC snapshot + memfile",
		zap.String("sandbox_id", sandboxID),
		zap.String("snap_path", snapPath),
		zap.String("mem_path", memPath))
	snapStart := time.Now()
	snapCtx, snapCancel := context.WithTimeout(ctx, 60*time.Second)
	defer snapCancel()
	// Dump BOTH snap and mem files. FC's MAP_PRIVATE on the memfd means CoW
	// writes never reach the memfd's pagecache; only mem_file_path gets us
	// FC's full live memory state for transfer.
	if err := sbx.CreateSnapshotWithMem(snapCtx, snapPath, memPath); err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "FC create snapshot+mem: %v", err))
	}
	snapfileBytes, err := os.ReadFile(snapPath)
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "read snapfile: %v", err))
	}
	memStat, err := os.Stat(memPath)
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "stat memfile: %v", err))
	}
	logger.L().Info(ctx, "PrepareMigrationSource: FC snapshot done",
		zap.String("sandbox_id", sandboxID),
		zap.Duration("elapsed", time.Since(snapStart)),
		zap.Int("snapfile_bytes", len(snapfileBytes)),
		zap.Int64("memfile_bytes", memStat.Size()))

	// Pass the memfile fd directly to rdma-source — no intermediate copy
	// to a hugetlbfs memfd. rdma-source's mmap path already falls back to
	// plain MAP_SHARED when MAP_HUGETLB fails. Tmpfs-backed memfile means
	// the data is already in RAM; ibv_reg_mr will pin the 4KB pages
	// (slower than hugepages but acceptable, saves the 2GB copy step).
	// Open RDWR because rdma-source mmaps with PROT_WRITE (kernel requires
	// matching fd permission). We don't actually write — only RDMA-Read out.
	memFile, err := os.OpenFile(memPath, os.O_RDWR, 0)
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "open memfile %s: %v", memPath, err))
	}
	sess.memfdFile = memFile

	logger.L().Info(ctx, "PrepareMigrationSource: spawning rdma-source",
		zap.String("sandbox_id", sandboxID),
		zap.String("binary", s.rdmaConfig.SourceBinary))
	spawnStart := time.Now()
	src, err := rdma.StartSource(context.WithoutCancel(ctx), s.rdmaConfig, sandboxID, memFile, uint64(pool.Size()))
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "start rdma-source: %v", err))
	}
	sess.source = src
	logger.L().Info(ctx, "PrepareMigrationSource: rdma-source spawned, waiting ready",
		zap.String("sandbox_id", sandboxID),
		zap.Duration("spawn_elapsed", time.Since(spawnStart)))

	readyStart := time.Now()
	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	qp, err := src.WaitReady(readyCtx)
	if err != nil {
		return cleanupOnError(status.Errorf(codes.Internal, "rdma-source not ready: %v", err))
	}
	logger.L().Info(ctx, "PrepareMigrationSource: rdma-source READY",
		zap.String("sandbox_id", sandboxID),
		zap.Duration("ready_elapsed", time.Since(readyStart)),
		zap.Int("tcp_port", qp.TCPPort))

	go s.watchSourceCompletion(context.WithoutCancel(ctx), sess)

	return &orchestrator.MigrationPrepareSourceResponse{
		RdmaAddr:  s.rdmaPublicAddr(),
		TcpPort:   uint32(qp.TCPPort),
		QpInfoHex: qp.Hex,
		Sandbox:   sbx.APIStoredConfig,
		Snapfile:  snapfileBytes,
	}, nil
}

// watchSourceCompletion blocks on rdma-source exit and SIGKILLs FC on a
// clean exit; on failure it leaves FC paused so AbortMigration can resume.
func (s *Server) watchSourceCompletion(ctx context.Context, sess *migrationSession) {
	err := sess.source.WaitDone(ctx)
	sess.doneOnce.Do(func() {
		sess.err = err
		close(sess.done)
	})
	if err != nil {
		sbxlogger.E(sess.sbx).Error(ctx, "rdma-source agent failed during migration",
			zap.Error(err), zap.String("sandbox_id", sess.sandboxID))
		return
	}
	if killErr := sess.sbx.Kill(ctx); killErr != nil {
		sbxlogger.E(sess.sbx).Error(ctx, "SIGKILL after migration commit failed",
			zap.Error(killErr), zap.String("sandbox_id", sess.sandboxID))
	}
	// Close runs the sandbox cleanup chain — releases page-pool refcount,
	// network slot, cgroup, etc. Without this the page-pool memfd leaks
	// (held with a "(deleted)" link by the orchestrator process even after
	// FC is killed), permanently consuming hugepages until orch restart.
	if closeErr := sess.sbx.Close(ctx); closeErr != nil {
		sbxlogger.E(sess.sbx).Error(ctx, "sandbox cleanup after migration commit failed",
			zap.Error(closeErr), zap.String("sandbox_id", sess.sandboxID))
	}
}

func (s *Server) CommitMigration(
	ctx context.Context,
	in *orchestrator.MigrationCommitRequest,
) (*emptypb.Empty, error) {
	sess := s.lookupMigration(in.GetSandboxId())
	if sess == nil {
		return nil, status.Errorf(codes.NotFound, "no migration in progress for %s", in.GetSandboxId())
	}
	select {
	case <-sess.done:
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "commit timed out before source finished")
	}
	defer s.cleanupSession(ctx, sess)
	if sess.err != nil {
		return nil, status.Errorf(codes.Internal, "migration failed: %v", sess.err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) AbortMigration(
	ctx context.Context,
	in *orchestrator.MigrationAbortRequest,
) (*emptypb.Empty, error) {
	sess := s.lookupMigration(in.GetSandboxId())
	if sess == nil {
		return &emptypb.Empty{}, nil // idempotent
	}
	logger.L().Warn(ctx, "migration aborted",
		zap.String("sandbox_id", sess.sandboxID),
		zap.String("reason", in.GetReason()))
	s.rollbackMigration(ctx, sess)
	return &emptypb.Empty{}, nil
}

func (s *Server) rollbackMigration(ctx context.Context, sess *migrationSession) {
	if sess.source != nil {
		_ = sess.source.Stop(ctx)
	}
	sess.doneOnce.Do(func() {
		sess.err = errors.New("migration rolled back")
		close(sess.done)
	})
	if sess.sbx != nil {
		if err := sess.sbx.ResumeFC(ctx); err != nil {
			sbxlogger.E(sess.sbx).Error(ctx, "resume FC after migration abort failed; killing",
				zap.Error(err), zap.String("sandbox_id", sess.sandboxID))
			_ = sess.sbx.Kill(ctx)
		}
	}
	s.cleanupSession(ctx, sess)
}

// cleanupSession closes the migration memfd we built from the dumped memfile
// (we own this one — it's NOT the page pool's memfd). Removes both snapfile
// and memfile from disk.
func (s *Server) cleanupSession(_ context.Context, sess *migrationSession) {
	s.clearMigration(sess.sandboxID)
	if sess.memfdFile != nil {
		_ = sess.memfdFile.Close()
	}
	if sess.snapfilePath != "" {
		_ = os.Remove(sess.snapfilePath)
	}
	if sess.memfilePath != "" {
		_ = os.Remove(sess.memfilePath)
	}
}

func (s *Server) rdmaPublicAddr() string {
	return s.config.RDMAAdvertiseAddr
}

// registerOOMSnapshotInAPI returns "" with nil error when apiClient is unset.
func (s *Server) registerOOMSnapshotInAPI(ctx context.Context, sbx *sandbox.Sandbox, oomBuildID string) (string, error) {
	if s.apiClient == nil {
		return "", nil
	}
	teamID, err := uuid.Parse(sbx.Runtime.TeamID)
	if err != nil {
		return "", fmt.Errorf("parse team_id: %w", err)
	}
	buildID, err := uuid.Parse(oomBuildID)
	if err != nil {
		return "", fmt.Errorf("parse build_id: %w", err)
	}

	regCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	resp, err := s.apiClient.RegisterOOMSnapshot(regCtx, apiclient.RegisterOOMSnapshotRequest{
		BuildID:            buildID,
		SandboxID:          sbx.Runtime.SandboxID,
		TeamID:             teamID,
		OriginNodeID:       env.GetNodeID(),
		Vcpu:               sbx.Config.Vcpu,
		RamMB:              sbx.Config.RamMB,
		TotalDiskSizeMB:    sbx.Config.TotalDiskSizeMB,
		KernelVersion:      sbx.Config.FirecrackerConfig.KernelVersion,
		FirecrackerVersion: sbx.Config.FirecrackerConfig.FirecrackerVersion,
		EnvdVersion:        sbx.Config.Envd.Version,
	})
	if err != nil {
		return "", err
	}
	return resp.SnapshotID, nil
}

// tryRDMAMigrationOnPressure returns true on successful migration — caller
// must NOT do anything further; the API has already triggered our
// CommitMigration which SIGKILL'd FC. Returns false on skip or failure
// (caller falls through to GCS snapshot path).
func (s *Server) tryRDMAMigrationOnPressure(ctx context.Context, sbx *sandbox.Sandbox) bool {
	if s.apiClient == nil || s.rdmaConfig.SourceBinary == "" {
		return false
	}
	if s.sandboxFactory.GetPagePool(sbx.Runtime.BuildID) == nil {
		return false
	}
	teamID, err := uuid.Parse(sbx.Runtime.TeamID)
	if err != nil {
		sbxlogger.E(sbx).Warn(ctx, "OOM rescue: skipping RDMA migration — bad team_id",
			zap.String("team_id", sbx.Runtime.TeamID), zap.Error(err))
		return false
	}

	migrateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 90*time.Second)
	defer cancel()

	t0 := time.Now()
	resp, err := s.apiClient.MigrateSandbox(migrateCtx, apiclient.MigrateSandboxRequest{
		TeamID:    teamID,
		SandboxID: sbx.Runtime.SandboxID,
	})
	if err != nil {
		sbxlogger.E(sbx).Warn(ctx, "OOM rescue: RDMA migration failed; falling back to GCS snapshot",
			zap.Duration("duration", time.Since(t0)),
			zap.Error(err),
		)
		return false
	}

	sbxlogger.E(sbx).Info(ctx, "OOM rescue: RDMA migration succeeded",
		zap.String("dest_node", resp.DestNodeID),
		zap.Int("duration_ms", resp.DurationMs),
	)
	return true
}

// ReceiveMigration blocks until the rdma-dest agent has finished prefetching.
func (s *Server) ReceiveMigration(
	ctx context.Context,
	in *orchestrator.MigrationReceiveRequest,
) (*orchestrator.MigrationReceiveResponse, error) {
	if s.rdmaConfig.DestBinary == "" {
		return nil, status.Error(codes.FailedPrecondition, "RDMA migration not configured (RDMA_DEST_BIN unset)")
	}

	src := in.GetSource()
	if src == nil || src.GetSandbox() == nil {
		return nil, status.Error(codes.InvalidArgument, "source response missing")
	}

	sbxConfig := src.GetSandbox()
	sandboxID := sbxConfig.GetSandboxId()
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox.sandbox_id is required")
	}
	logger.L().Info(ctx, "ReceiveMigration: enter",
		zap.String("sandbox_id", sandboxID),
		zap.String("src_addr", src.GetRdmaAddr()),
		zap.Uint32("src_tcp_port", src.GetTcpPort()))

	tpl, err := s.templateCache.GetTemplate(
		ctx,
		sbxConfig.GetBuildId(),
		true,
		false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: sbxConfig.GetMaxSandboxLength()},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get template for migration: %v", err)
	}
	logger.L().Info(ctx, "ReceiveMigration: template loaded",
		zap.String("sandbox_id", sandboxID),
		zap.String("build_id", sbxConfig.GetBuildId()))

	internalCfg, err := buildMigrationSandboxConfig(sbxConfig, s.config.AllowSandboxInternet)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sandbox config: %v", err)
	}

	runtime := sandbox.RuntimeMetadata{
		TemplateID:  sbxConfig.GetTemplateId(),
		SandboxID:   sandboxID,
		ExecutionID: sbxConfig.GetExecutionId(),
		TeamID:      sbxConfig.GetTeamId(),
		BuildID:     sbxConfig.GetBuildId(),
		SandboxType: sandbox.SandboxTypeSandbox,
	}

	var (
		mu               sync.Mutex
		destAgent        *rdma.Dest
		destFaultsHandled uint64
	)

	startedAt := in.GetStartTime().AsTime()
	endAt := in.GetEndTime().AsTime()

	mc := sandbox.MigrationReceiveConfig{
		Template:        tpl,
		SandboxConfig:   internalCfg,
		Runtime:         runtime,
		StartedAt:       startedAt,
		EndAt:           endAt,
		SnapfileBytes:   src.GetSnapfile(),
		APIStoredConfig: sbxConfig,
		HandoffUFFD: func(handoffCtx context.Context, uffdFd uintptr, mapping *memory.Mapping) error {
			if len(mapping.Regions) == 0 {
				return errors.New("no memory regions reported by FC")
			}
			fcBaseVA := uint64(mapping.Regions[0].BaseHostVirtAddr)
			sizeBytes := uint64(mapping.Regions[0].Size)
			if len(mapping.Regions) > 1 {
				logger.L().Warn(handoffCtx, "multi-region sandbox; MVP only handles first region",
					zap.Int("regions", len(mapping.Regions)),
					zap.String("sandbox_id", sandboxID))
			}

			pool := s.sandboxFactory.GetPagePool(sbxConfig.GetBuildId())
			if pool == nil {
				return fmt.Errorf("page pool for build %s missing", sbxConfig.GetBuildId())
			}
			memfdFile := os.NewFile(uintptr(pool.MemfdFd()), fmt.Sprintf("memfd-%s", sandboxID))
			if memfdFile == nil {
				return fmt.Errorf("wrap memfd fd %d", pool.MemfdFd())
			}

			logger.L().Info(handoffCtx, "ReceiveMigration: HandoffUFFD invoked, starting rdma-dest",
				zap.String("sandbox_id", sandboxID),
				zap.Uintptr("uffd_fd", uffdFd),
				zap.Int("memfd_fd", pool.MemfdFd()),
				zap.Uint64("fc_base_va", fcBaseVA),
				zap.Uint64("size", sizeBytes))

			uffdFile := os.NewFile(uffdFd, "uffd")
			if uffdFile == nil {
				return fmt.Errorf("wrap uffd fd %d", uffdFd)
			}

			endpoint := rdma.SourceEndpoint{
				Addr:     src.GetRdmaAddr(),
				TCPPort:  int(src.GetTcpPort()),
				QPInfoHx: src.GetQpInfoHex(),
			}

			d, err := rdma.StartDest(handoffCtx, s.rdmaConfig, sandboxID, uffdFile, memfdFile, sizeBytes, fcBaseVA, endpoint)
			if err != nil {
				_ = uffdFile.Close()
				return fmt.Errorf("start rdma-dest: %w", err)
			}
			logger.L().Info(handoffCtx, "ReceiveMigration: rdma-dest spawned, waiting RTS",
				zap.String("sandbox_id", sandboxID))

			rtsCtx, rtsCancel := context.WithTimeout(handoffCtx, 20*time.Second)
			if err := d.WaitRTS(rtsCtx); err != nil {
				rtsCancel()
				_ = d.Stop(handoffCtx)
				return fmt.Errorf("rdma-dest QP RTS: %w", err)
			}
			rtsCancel()
			logger.L().Info(handoffCtx, "ReceiveMigration: rdma-dest QP RTS",
				zap.String("sandbox_id", sandboxID))

			// Block here until rdma-dest finishes prefetch AND installs PT
			// entries for all hugepages. Without this gate, FC.resumeVM kicks
			// virtio devices and reads queue addresses while pagecache is only
			// half-populated, panicking on inconsistent avail/used indices.
			// Effectively pre-copy: FC's resumeVM only proceeds after all
			// guest memory is materialized in dest's memfd pagecache.
			logger.L().Info(handoffCtx, "ReceiveMigration: blocking until prefetch + PT install complete",
				zap.String("sandbox_id", sandboxID))
			doneCtx, doneCancel := context.WithTimeout(handoffCtx, 5*time.Minute)
			faults, err := d.WaitDone(doneCtx)
			doneCancel()
			if err != nil {
				_ = d.Stop(handoffCtx)
				return fmt.Errorf("rdma-dest prefetch: %w", err)
			}
			logger.L().Info(handoffCtx, "ReceiveMigration: prefetch DONE — FC can now resume safely",
				zap.String("sandbox_id", sandboxID),
				zap.Uint64("faults_handled", faults))

			mu.Lock()
			destAgent = d
			destFaultsHandled = faults
			mu.Unlock()
			return nil
		},
	}

	logger.L().Info(ctx, "ReceiveMigration: calling sandboxFactory.ReceiveMigration",
		zap.String("sandbox_id", sandboxID))
	sbx, err := s.sandboxFactory.ReceiveMigration(ctx, mc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "receive migration: %v", err)
	}
	logger.L().Info(ctx, "ReceiveMigration: sandbox created, waiting prefetch DONE",
		zap.String("sandbox_id", sandboxID))

	// Wire the dest sandbox into the lifecycle handler. Without this, when the
	// sandbox eventually exits (user kill, timeout, OOM) nothing calls
	// sbx.Close() → cleanup chain doesn't run → page-pool refcount never drops
	// → memfd "(deleted)" stays held by orchestrator with hugepages pinned.
	// Same mechanism used by Create/Resume in setupSandboxLifecycle.
	s.setupSandboxLifecycle(ctx, sbx)

	mu.Lock()
	d := destAgent
	faults := destFaultsHandled
	mu.Unlock()
	if d == nil {
		return nil, status.Error(codes.Internal, "rdma-dest agent not started; handoff misconfigured")
	}

	// rdma-dest already finished (waited in HandoffUFFD). Just collect stats.
	done, total := d.Progress()

	logger.L().Info(ctx, "migration received",
		zap.String("sandbox_id", sandboxID),
		zap.Uint64("pages_pulled", done),
		zap.Uint64("pages_total", total),
		zap.Uint64("faults_handled", faults),
	)

	_ = sbx // sandbox stays in the factory's map under sandboxID

	return &orchestrator.MigrationReceiveResponse{
		ClientId:           s.info.ClientId,
		PrefetchPagesDone:  done,
		PrefetchPagesTotal: total,
		FaultsHandled:      faults,
	}, nil
}

// buildMigrationSandboxConfig trusts the wire config — internet policy
// is already enforced at the API layer.
func buildMigrationSandboxConfig(p *orchestrator.SandboxConfig, _ bool) (*sandbox.Config, error) {
	if p == nil {
		return nil, errors.New("nil sandbox config")
	}
	return sandbox.NewConfig(sandbox.Config{
		BaseTemplateID: p.GetBaseTemplateId(),
		Vcpu:           p.GetVcpu(),
		RamMB:          p.GetRamMb(),
		HugePages:      p.GetHugePages(),
		FirecrackerConfig: fc.Config{
			KernelVersion:      p.GetKernelVersion(),
			FirecrackerVersion: p.GetFirecrackerVersion(),
		},
		Envd:                  sandbox.EnvdMetadata{Version: p.GetEnvdVersion()},
		MaxSandboxLengthHours: p.GetMaxSandboxLength(),
		Network:               p.GetNetwork(),
	}), nil
}
