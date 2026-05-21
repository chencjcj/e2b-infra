// Package pressure stops the highest-hugepage-consuming sandbox when the
// node's hugetlb pool crosses a hard watermark. Stop (not pause+snapshot):
// snapshotting itself needs pages and a partial snapshot would poison the
// template cache.
package pressure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/procstats"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	DefaultHardWatermark = 0.90
	// Tick and cooldown stay under the UFFD stall budget so eviction reacts
	// before stalled page-fault handlers exhaust their retry window.
	DefaultTickInterval   = 1 * time.Second
	DefaultActionCooldown = 5 * time.Second

	EvictReason = "memory_pressure"
)

// StopFunc is injected to avoid a pkg/pressure → pkg/server import cycle.
type StopFunc func(ctx context.Context, sbx *sandbox.Sandbox, extraEventData map[string]any) error

type hugepageReader interface {
	GetHugepageMetrics() (*metrics.HugepageMetrics, error)
}

type Monitor struct {
	sandboxes   *sandbox.Map
	hostMetrics hugepageReader
	stopFn      StopFunc

	hardWatermark  float64
	tickInterval   time.Duration
	actionCooldown time.Duration

	readHugetlbFn func(pid int) (uint64, error)
	now           func() time.Time

	mu           sync.Mutex
	lastActionAt time.Time

	closeOnce sync.Once
	closed    chan struct{}
}

// Options: zero fields fall back to Default* constants.
type Options struct {
	HardWatermark  float64
	TickInterval   time.Duration
	ActionCooldown time.Duration
}

func NewMonitor(sandboxes *sandbox.Map, hostMetrics *metrics.HostMetrics, stopFn StopFunc, opts Options) *Monitor {
	return newMonitor(sandboxes, hostMetrics, stopFn, opts)
}

func newMonitor(sandboxes *sandbox.Map, hostMetrics hugepageReader, stopFn StopFunc, opts Options) *Monitor {
	if opts.HardWatermark <= 0 {
		opts.HardWatermark = DefaultHardWatermark
	}
	if opts.TickInterval <= 0 {
		opts.TickInterval = DefaultTickInterval
	}
	if opts.ActionCooldown <= 0 {
		opts.ActionCooldown = DefaultActionCooldown
	}

	return &Monitor{
		sandboxes:      sandboxes,
		hostMetrics:    hostMetrics,
		stopFn:         stopFn,
		hardWatermark:  opts.HardWatermark,
		tickInterval:   opts.TickInterval,
		actionCooldown: opts.ActionCooldown,
		readHugetlbFn:  procstats.ReadHugetlbBytes,
		now:            time.Now,
		closed:         make(chan struct{}),
	}
}

func (m *Monitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(m.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-m.closed:
			return nil
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *Monitor) Close(_ context.Context) error {
	m.closeOnce.Do(func() { close(m.closed) })
	return nil
}

func (m *Monitor) tick(ctx context.Context) {
	hp, err := m.hostMetrics.GetHugepageMetrics()
	if err != nil {
		logger.L().Warn(ctx, "pressure monitor: failed to read hugepage metrics", zap.Error(err))
		return
	}
	if hp == nil || hp.TotalBytes == 0 {
		return
	}

	used := hp.TotalBytes - hp.FreeBytes
	usage := float64(used) / float64(hp.TotalBytes)
	if usage < m.hardWatermark {
		return
	}

	m.mu.Lock()
	elapsed := m.now().Sub(m.lastActionAt)
	inCooldown := !m.lastActionAt.IsZero() && elapsed < m.actionCooldown
	m.mu.Unlock()
	if inCooldown {
		logger.L().Debug(ctx, "pressure monitor: in cooldown, skipping",
			zap.Float64("usage", usage),
			zap.Duration("elapsed", elapsed),
		)
		return
	}

	victim, victimHugetlb := m.pickVictim(ctx)
	if victim == nil {
		logger.L().Warn(ctx, "pressure monitor: watermark exceeded but no victim found",
			zap.Float64("usage", usage),
			zap.Uint64("total_bytes", hp.TotalBytes),
			zap.Uint64("free_bytes", hp.FreeBytes),
		)
		return
	}

	sandboxID := victim.Runtime.SandboxID
	logger.L().Warn(ctx, "pressure monitor: stopping sandbox to reclaim hugepages",
		zap.Float64("usage", usage),
		zap.Float64("hard_watermark", m.hardWatermark),
		zap.String("victim_sandbox_id", sandboxID),
		zap.Uint64("victim_hugetlb_bytes", victimHugetlb),
	)

	// Reset cooldown even on failure — prevents pile-up of retries on a sustained problem.
	m.mu.Lock()
	m.lastActionAt = m.now()
	m.mu.Unlock()

	extra := map[string]any{
		"reason": EvictReason,
	}
	// WithoutCancel: ticker cancellation must not abort the background Stop goroutine.
	stopErr := m.stopFn(context.WithoutCancel(ctx), victim, extra)
	if stopErr != nil {
		logger.L().Error(ctx, "pressure monitor: stop call failed",
			zap.String("sandbox_id", sandboxID),
			zap.Error(stopErr),
		)
		return
	}
}

func (m *Monitor) pickVictim(ctx context.Context) (*sandbox.Sandbox, uint64) {
	items := m.sandboxes.Items()
	if len(items) == 0 {
		return nil, 0
	}

	type candidate struct {
		sbx   *sandbox.Sandbox
		bytes uint64
	}
	candidates := make([]candidate, 0, len(items))
	for _, sbx := range items {
		pid, err := sbx.Pid()
		if err != nil {
			continue
		}
		bytes, rerr := m.readHugetlbFn(pid)
		if rerr != nil {
			logger.L().Debug(ctx, "pressure monitor: failed to read hugetlb",
				zap.String("sandbox_id", sbx.Runtime.SandboxID),
				zap.Int("pid", pid),
				zap.Error(rerr),
			)
			continue
		}
		if bytes == 0 {
			continue
		}
		candidates = append(candidates, candidate{sbx, bytes})
	}
	if len(candidates) == 0 {
		return nil, 0
	}

	// Highest hugetlb first; tie-break by ID for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].bytes != candidates[j].bytes {
			return candidates[i].bytes > candidates[j].bytes
		}
		return candidates[i].sbx.Runtime.SandboxID < candidates[j].sbx.Runtime.SandboxID
	})
	return candidates[0].sbx, candidates[0].bytes
}

var ErrStopFuncRequired = errors.New("pressure monitor: StopFunc is required")

func (m *Monitor) Validate() error {
	if m.stopFn == nil {
		return ErrStopFuncRequired
	}
	if m.sandboxes == nil {
		return fmt.Errorf("pressure monitor: sandbox map is required")
	}
	if m.hostMetrics == nil {
		return fmt.Errorf("pressure monitor: host metrics source is required")
	}
	return nil
}
