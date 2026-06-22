package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type MigrationReceiveConfig struct {
	Template        template.Template
	SandboxConfig   *Config
	Runtime         RuntimeMetadata
	StartedAt       time.Time
	EndAt           time.Time
	SnapfileBytes   []byte
	APIStoredConfig *orchestrator.SandboxConfig

	// HandoffUFFD takes ownership of uffdFd, spawns rdma-dest, and blocks
	// until QP is in RTS. Only then does FC's resume path proceed.
	HandoffUFFD func(ctx context.Context, uffdFd uintptr, mapping *memory.Mapping) error
}

// ReceiveMigration parallels ResumeSandbox but uses RDMAHandoff instead of
// the in-process Userfaultfd.Serve, and reads its snapfile from bytes.
func (f *Factory) ReceiveMigration(ctx context.Context, mc MigrationReceiveConfig) (s *Sandbox, e error) {
	ctx, span := tracer.Start(ctx, "receive migration")
	defer span.End()
	defer handleSpanError(span, &e)

	if mc.Template == nil {
		return nil, errors.New("MigrationReceiveConfig.Template is nil")
	}
	if mc.SandboxConfig == nil {
		return nil, errors.New("MigrationReceiveConfig.SandboxConfig is nil")
	}
	if mc.HandoffUFFD == nil {
		return nil, errors.New("MigrationReceiveConfig.HandoffUFFD is nil")
	}

	execCtx, execSpan := startExecutionSpan(ctx)
	exit := utils.NewErrorOnce()
	cleanup := NewCleanup()
	defer func() {
		if e != nil {
			cleanupErr := cleanup.Run(ctx)
			e = errors.Join(e, cleanupErr)
			handleSpanError(execSpan, &e)
			execSpan.End()
		}
	}()

	t := mc.Template
	config := mc.SandboxConfig
	runtime := mc.Runtime

	lifecycleID := uuid.NewString()

	sandboxFiles := t.Files().NewSandboxFiles(runtime.SandboxID)
	cleanup.Add(ctx, cleanupFiles(f.config, sandboxFiles))

	snapPath := filepath.Join(f.config.OrchestratorBaseDir, "tmp",
		fmt.Sprintf("migrate-recv-%s.snap", runtime.SandboxID))
	if err := os.MkdirAll(filepath.Dir(snapPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snap tmp: %w", err)
	}
	if err := os.WriteFile(snapPath, mc.SnapfileBytes, 0o600); err != nil {
		return nil, fmt.Errorf("write snapfile: %w", err)
	}
	snapfile := template.NewLocalFileLink(snapPath)
	cleanup.AddNoContext(ctx, snapfile.Close)

	memfile, err := t.Memfile(ctx)
	if err != nil {
		return nil, fmt.Errorf("get template memfile: %w", err)
	}
	memfileSize, err := memfile.Size(ctx)
	if err != nil {
		return nil, fmt.Errorf("get memfile size: %w", err)
	}
	enableSharedMemory := f.config.EnableSharedMemory
	isBuildSandbox := runtime.SandboxType == SandboxTypeBuild
	var sharedMemfdPath string
	if config.HugePages && enableSharedMemory && !isBuildSandbox {
		pool, err := f.acquirePagePool(runtime.BuildID, memfileSize, memfile.BlockSize(), config.HugePages)
		if err != nil {
			return nil, fmt.Errorf("acquire page pool: %w", err)
		}
		poolBuildID := runtime.BuildID
		cleanup.AddNoContext(ctx, func() error {
			f.releasePagePool(poolBuildID)
			return nil
		})
		sharedMemfdPath = pool.MemfdPath()
		logger.L().Info(ctx, "migration dest: page pool ready",
			zap.String("memfd_path", sharedMemfdPath),
			zap.Int("memfd_fd", pool.MemfdFd()),
			zap.Int64("size", memfileSize))
	} else {
		return nil, fmt.Errorf("migration requires HugePages + EnableSharedMemory")
	}

	ipsPromise := getNetworkSlot(ctx, f.networkPool, cleanup, config.Network, f.Sandboxes.NetworkReleased)

	overlayPromise := utils.NewPromise(func() (rootfs.Provider, error) {
		readonlyRootfs, err := t.Rootfs()
		if err != nil {
			return nil, fmt.Errorf("get rootfs: %w", err)
		}
		overlay, err := rootfs.NewNBDProvider(
			ctx,
			readonlyRootfs,
			sandboxFiles.SandboxCacheRootfsPath(f.config.StorageConfig),
			f.devicePool,
			f.featureFlags,
		)
		if err != nil {
			return nil, fmt.Errorf("create rootfs overlay: %w", err)
		}
		cleanup.Add(ctx, overlay.Close)
		go func() {
			if runErr := overlay.Start(execCtx); runErr != nil {
				logger.L().Error(ctx, "rootfs overlay error", zap.Error(runErr))
			}
		}()
		return overlay, nil
	})

	fcUffdPath := sandboxFiles.SandboxUffdSocketPath()
	handoff := uffd.NewRDMAHandoff(memfileSize, memfile.BlockSize(), fcUffdPath)
	handoff.OnReceived = mc.HandoffUFFD

	if err := handoff.Start(ctx, runtime.SandboxID); err != nil {
		return nil, fmt.Errorf("start rdma handoff: %w", err)
	}
	cleanup.AddNoContext(ctx, handoff.Stop)

	ips, err := ipsPromise.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("network slot: %w", err)
	}
	overlay, err := overlayPromise.Wait(ctx)
	if err != nil {
		return nil, err
	}

	rootfsDev, err := t.Rootfs()
	if err != nil {
		return nil, fmt.Errorf("get rootfs dev: %w", err)
	}
	meta, err := t.Metadata()
	if err != nil {
		return nil, fmt.Errorf("get metadata: %w", err)
	}
	cgroupHandle, cgroupFD := createCgroup(ctx, f.cgroupManager, sandboxFiles.SandboxCgroupName(),
		cgroupHugetlbLimitMB(f.config.EnableSharedMemory, config))
	defer releaseCgroupFD(ctx, cgroupHandle, runtime.SandboxID)
	cleanup.Add(ctx, func(ctx context.Context) error { return cgroupHandle.Remove(ctx) })

	fcHandle, fcErr := fc.NewProcess(
		ctx, execCtx, f.config, ips, sandboxFiles,
		config.FirecrackerConfig, overlay,
		fc.RootfsPaths{
			TemplateVersion: meta.Version,
			TemplateID:      config.BaseTemplateID,
			BuildID:         rootfsDev.Header().Metadata.BaseBuildId.String(),
		},
	)
	if fcErr != nil {
		return nil, fmt.Errorf("create fc: %w", fcErr)
	}

	resumeThrottle := featureflags.GetTCPFirewallEgressThrottleConfig(ctx, f.featureFlags)
	resumeDriveThrottle := featureflags.GetBlockDriveThrottleConfig(ctx, f.featureFlags)

	resources := &Resources{
		Slot:   ips,
		memory: handoff,
	}
	metadata := &Metadata{
		internalConfig: internalConfig{EnvdInitRequestTimeout: f.GetEnvdInitRequestTimeout(ctx)},
		Config:         config,
		Runtime:        runtime,
		startedAt:      mc.StartedAt,
		endAt:          mc.EndAt,
	}
	sbx := &Sandbox{
		LifecycleID:     lifecycleID,
		Resources:       resources,
		Metadata:        metadata,
		cgroupHandle:    cgroupHandle,
		Template:        t,
		config:          f.config,
		files:           sandboxFiles,
		process:         fcHandle,
		cleanup:         cleanup,
		APIStoredConfig: mc.APIStoredConfig,
		CABundle:        f.egressProxy.CABundle(),
		exit:            exit,
	}
	useClickhouseMetrics := f.featureFlags.BoolFlag(ctx, featureflags.MetricsWriteFlag)
	sbx.Checks = NewChecks(sbx, useClickhouseMetrics)

	cleanup.AddPriority(ctx, func(ctx context.Context) error { return sbx.Stop(ctx) })

	f.Sandboxes.AssignNetwork(ctx, sbx)
	cleanup.Add(ctx, func(ctx context.Context) error {
		f.Sandboxes.MarkStopping(ctx, runtime.SandboxID, sbx.LifecycleID)
		return nil
	})

	samplingInterval := time.Duration(f.featureFlags.IntFlag(execCtx, featureflags.HostStatsSamplingInterval)) * time.Millisecond
	initializeHostStatsCollector(execCtx, sbx, runtime, config, f.hostStatsDelivery, samplingInterval)
	cleanup.Add(ctx, func(ctx context.Context) error {
		if sbx.hostStatsCollector != nil {
			sbx.hostStatsCollector.Stop(ctx)
		}
		return nil
	})

	uffdStartCtx, cancelUffdStartCtx := context.WithCancelCause(ctx)
	defer cancelUffdStartCtx(errors.New("uffd finished starting"))
	go func() {
		exitErr := handoff.Exit().Wait()
		cancelUffdStartCtx(fmt.Errorf("rdma handoff exited: %w",
			errors.Join(exitErr, context.Cause(uffdStartCtx))))
	}()

	if err := fcHandle.Resume(
		uffdStartCtx,
		sbxlogger.SandboxMetadata{
			SandboxID:  runtime.SandboxID,
			TemplateID: runtime.TemplateID,
			TeamID:     runtime.TeamID,
		},
		fcUffdPath,
		snapfile,
		handoff.Ready(),
		config.Envd.AccessToken,
		cgroupFD,
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(resumeThrottle.Ops),
			Bandwidth: fc.TokenBucketConfig(resumeThrottle.Bandwidth),
		},
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(resumeDriveThrottle.Ops),
			Bandwidth: fc.TokenBucketConfig(resumeDriveThrottle.Bandwidth),
		},
		sharedMemfdPath,
	); err != nil {
		return nil, fmt.Errorf("fc resume: %w", err)
	}

	if err := sbx.WaitForEnvd(ctx, f.config.EnvdTimeout); err != nil {
		return nil, fmt.Errorf("wait for envd: %w", err)
	}
	f.Sandboxes.MarkRunning(ctx, sbx)
	go sbx.Checks.Start(execCtx)

	go func() {
		defer execSpan.End()
		ctx, span := tracer.Start(execCtx, "migrated-sandbox-exit-wait")
		defer span.End()
		select {
		case <-handoff.Exit().Done():
		case <-fcHandle.Exit.Done():
		}
		stopErr := sbx.Stop(ctx)
		uffdErr := handoff.Exit().Wait()
		fcErr := fcHandle.Exit.Wait()
		exit.SetError(errors.Join(stopErr, fcErr, uffdErr))
	}()

	return sbx, nil
}
