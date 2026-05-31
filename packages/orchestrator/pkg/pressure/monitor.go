// Package pressure is the per-node hugepage memory-pressure controller.
// See docs/design-memory-pressure-scheduling.md for the architecture.
package pressure

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/procstats"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const EvictReason = "memory_pressure"

// StopFunc is injected to avoid a pkg/pressure → pkg/server import cycle.
type StopFunc func(ctx context.Context, sbx *sandbox.Sandbox, extraEventData map[string]any) error

type hugepageReader interface {
	GetHugepageMetrics() (*metrics.HugepageMetrics, error)
}

// freshHugepageReader parses /proc/meminfo on every call. Eviction must use
// this — its post-kill drain detection (interKillDelay 500ms, stagnantWindow
// 2s) can't work against the HostMetrics 10s-ticker cache.
type freshHugepageReader struct{}

func (freshHugepageReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	total, free, err := procstats.ReadHugepagePool()
	if err != nil {
		return nil, err
	}
	return &metrics.HugepageMetrics{TotalBytes: total, FreeBytes: free}, nil
}

// Options zero fields fall back to defaults in params.go.
type Options struct {
	SampleIdle  time.Duration
	SampleWarm  time.Duration
	SampleHot   time.Duration
	SamplePanic time.Duration

	WatermarkRecomputeInterval time.Duration

	EvictTriggerFrac           float64
	EvictStopFrac              float64
	EvictHotzoneFrac           float64
	EvictPanicFrac             float64
	EvictPostBatchCooldown     time.Duration
	EvictInterKillDelay        time.Duration
	EvictMaxKillsPerRound      int
	EvictStagnantWindow        time.Duration
	EvictTickIdle              time.Duration
	EvictTickHotzone           time.Duration
	EvictTickPanic             time.Duration

	// Meter is optional; nil disables eviction counters/histograms.
	Meter metric.Meter
}

type Monitor struct {
	sandboxes   *sandbox.Map
	hostMetrics *metrics.HostMetrics
	stopFn      StopFunc

	rate      *RateEstimator
	watermark *WatermarkController
	eviction  *Eviction

	opts Options

	// Owned by the sampler goroutine.
	lastFreeBytes  uint64
	lastSampleTime time.Time
	hasLastSample  bool

	// Published outputs read by ServiceInfo. atomic.Uint64 carries float64 bits.
	publishedT     atomic.Uint64
	publishedRate  atomic.Uint64
	publishedTTE   atomic.Uint64
	publishedFree  atomic.Uint64
	publishedTotal atomic.Uint64

	readPoolFn func() (uint64, uint64, error)
	now        func() time.Time

	watermarkGauge   metric.Float64ObservableGauge
	rateGauge        metric.Float64ObservableGauge
	tteGauge         metric.Float64ObservableGauge
	registeredCallbk metric.Registration

	closeOnce sync.Once
	closed    chan struct{}
}

func NewMonitor(sandboxes *sandbox.Map, hostMetrics *metrics.HostMetrics, stopFn StopFunc, opts Options) *Monitor {
	applyOptionDefaults(&opts)

	m := &Monitor{
		sandboxes:   sandboxes,
		hostMetrics: hostMetrics,
		stopFn:      stopFn,
		rate:        NewRateEstimator(),
		watermark:   NewWatermarkController(),
		opts:        opts,
		readPoolFn:  procstats.ReadHugepagePool,
		now:         time.Now,
		closed:      make(chan struct{}),
	}
	m.eviction = NewEviction(sandboxes, freshHugepageReader{}, stopFn, opts.Meter, EvictionOptions{
		TriggerFrac:           opts.EvictTriggerFrac,
		StopFrac:              opts.EvictStopFrac,
		HotzoneFrac:           opts.EvictHotzoneFrac,
		PanicFrac:             opts.EvictPanicFrac,
		PostBatchCooldown:     opts.EvictPostBatchCooldown,
		InterKillDelay:        opts.EvictInterKillDelay,
		MaxKillsPerRound:      opts.EvictMaxKillsPerRound,
		StagnantWindow:        opts.EvictStagnantWindow,
		TickIdle:              opts.EvictTickIdle,
		TickHotzone:           opts.EvictTickHotzone,
		TickPanic:             opts.EvictTickPanic,
	})

	// Publish the construction-time placeholder (tMax) so ServiceInfo callers
	// before the first watermark tick get a sane value rather than zero.
	m.storePublished(m.watermark.Current(), 0, 0, 0, 0)

	if opts.Meter != nil {
		m.installMetrics(opts.Meter)
	}
	return m
}

func applyOptionDefaults(o *Options) {
	if o.SampleIdle <= 0 {
		o.SampleIdle = sampleIdle
	}
	if o.SampleWarm <= 0 {
		o.SampleWarm = sampleWarm
	}
	if o.SampleHot <= 0 {
		o.SampleHot = sampleHot
	}
	if o.SamplePanic <= 0 {
		o.SamplePanic = samplePanic
	}
	if o.WatermarkRecomputeInterval <= 0 {
		o.WatermarkRecomputeInterval = watermarkRecomputeInterval
	}
	if o.EvictTriggerFrac <= 0 {
		o.EvictTriggerFrac = evictTriggerFrac
	}
	if o.EvictStopFrac <= 0 {
		o.EvictStopFrac = evictStopFrac
	}
	if o.EvictHotzoneFrac <= 0 {
		o.EvictHotzoneFrac = evictHotzoneFrac
	}
	if o.EvictPostBatchCooldown <= 0 {
		o.EvictPostBatchCooldown = postBatchCooldown
	}
	if o.EvictInterKillDelay <= 0 {
		o.EvictInterKillDelay = interKillDelay
	}
	if o.EvictMaxKillsPerRound <= 0 {
		o.EvictMaxKillsPerRound = maxKillsPerRound
	}
	if o.EvictStagnantWindow <= 0 {
		o.EvictStagnantWindow = stagnantWindow
	}
	if o.EvictTickIdle <= 0 {
		o.EvictTickIdle = evictTickIdle
	}
	if o.EvictTickHotzone <= 0 {
		o.EvictTickHotzone = evictTickHotzone
	}
}

func (m *Monitor) Watermark() float64 {
	return math.Float64frombits(m.publishedT.Load())
}

func (m *Monitor) PredictedRateBytesPerSec() float64 {
	return math.Float64frombits(m.publishedRate.Load())
}

// WakeEviction triggers an immediate rescue tick. Safe and idempotent.
// Used by UFFD ENOMEM to preempt the sampler tick.
func (m *Monitor) WakeEviction() {
	m.eviction.Wake()
}

// Start blocks until ctx is cancelled or Close is called.
func (m *Monitor) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		m.runSampler(ctx)
	}()
	go func() {
		defer wg.Done()
		m.runWatermark(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = m.eviction.Start(ctx)
	}()

	wg.Wait()
	return nil
}

// Close is idempotent.
func (m *Monitor) Close(ctx context.Context) error {
	m.closeOnce.Do(func() {
		close(m.closed)
	})
	if m.registeredCallbk != nil {
		_ = m.registeredCallbk.Unregister()
		m.registeredCallbk = nil
	}
	_ = m.eviction.Close(ctx)
	return nil
}

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

var ErrStopFuncRequired = errors.New("pressure monitor: StopFunc is required")

func (m *Monitor) runSampler(ctx context.Context) {
	timer := time.NewTimer(m.opts.SampleIdle)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.closed:
			return
		case <-timer.C:
			next := m.sample(ctx)
			timer.Reset(next)
		}
	}
}

func (m *Monitor) sample(ctx context.Context) time.Duration {
	total, free, err := m.readPoolFn()
	if err != nil {
		logger.L().Debug(ctx, "pressure sampler: failed to read /proc/meminfo", zap.Error(err))
		return m.opts.SampleIdle
	}
	if total == 0 {
		return m.opts.SampleIdle
	}

	now := m.now()
	if m.hasLastSample {
		dt := now.Sub(m.lastSampleTime).Seconds()
		if dt > 0 {
			// Free dropping = pool growing in usage. Negative deltas (pages
			// released) are clamped to 0 inside RateEstimator.Update.
			rate := float64(int64(m.lastFreeBytes)-int64(free)) / dt
			m.rate.Update(rate)
		}
	}
	m.lastFreeBytes = free
	m.lastSampleTime = now
	m.hasLastSample = true

	used := total - free
	usage := float64(used) / float64(total)

	// Wake eviction so it doesn't wait up to tickHotzone/tickIdle for the
	// next tick — would burn into the 5s UFFD stall budget.
	if usage >= m.eviction.triggerFrac {
		m.eviction.Wake()
	}

	switch {
	case usage >= samplePanicFrac:
		return m.opts.SamplePanic
	case usage >= sampleHotFrac:
		return m.opts.SampleHot
	case usage >= sampleWarmFrac:
		return m.opts.SampleWarm
	default:
		return m.opts.SampleIdle
	}
}

func (m *Monitor) runWatermark(ctx context.Context) {
	ticker := time.NewTicker(m.opts.WatermarkRecomputeInterval)
	defer ticker.Stop()

	// Compute once immediately so ServiceInfo callers don't see the
	// construction-time placeholder for a full tick interval.
	m.recomputeWatermark(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.closed:
			return
		case <-ticker.C:
			m.recomputeWatermark(ctx)
		}
	}
}

func (m *Monitor) recomputeWatermark(ctx context.Context) {
	hp, err := m.hostMetrics.GetHugepageMetrics()
	if err != nil || hp == nil {
		return
	}
	if hp.TotalBytes == 0 {
		// No pool — admission fully open.
		m.storePublished(1.0, 0, 0, hp.FreeBytes, hp.TotalBytes)
		return
	}

	actualHugetlb := m.eviction.ActualPrivateBytes()
	rate := m.rate.Predict()
	// Feed the cumulative kill counter into the watermark before computing T.
	// Each kill within the 10 min window shaves 0.05 off tMax (capped at 0.30),
	// tightening admission until the kill ages out of the sliding window.
	m.watermark.RecordOOMCount(m.eviction.KillCount())
	target := ComputeTarget(hp.TotalBytes, hp.FreeBytes, actualHugetlb, rate)
	t := m.watermark.Tick(target)

	tteSec := 0.0
	if rate > 0 {
		tteSec = float64(hp.FreeBytes) / rate
	}

	m.storePublished(t, rate, tteSec, hp.FreeBytes, hp.TotalBytes)

	usagePct := float64(hp.TotalBytes-hp.FreeBytes) / float64(hp.TotalBytes) * 100

	round2 := func(v float64) float64 { return math.Round(v*100) / 100 }
	rateMBps := rate / (1024 * 1024)
	if rateMBps < 0.01 {
		rateMBps = 0
	}
	tteDisplay := round2(tteSec)
	if rate < 1 || math.IsInf(tteSec, 0) || tteSec > 1e9 {
		tteDisplay = math.Inf(1)
	}

	logger.L().Info(ctx, "pressure: watermark recomputed",
		zap.Float64("usage_pct", round2(usagePct)),
		zap.Float64("watermark_pct", round2(t*100)),
		zap.Float64("target_pct", round2(target*100)),
		zap.Uint64("free_bytes", hp.FreeBytes),
		zap.Uint64("total_bytes", hp.TotalBytes),
		zap.Uint64("actual_hugetlb_bytes", actualHugetlb),
		zap.Float64("rate_mbps", round2(rateMBps)),
		zap.Float64("tte_seconds", tteDisplay),
	)
}

func (m *Monitor) storePublished(t, rate, tte float64, free, total uint64) {
	m.publishedT.Store(math.Float64bits(t))
	m.publishedRate.Store(math.Float64bits(rate))
	m.publishedTTE.Store(math.Float64bits(tte))
	m.publishedFree.Store(free)
	m.publishedTotal.Store(total)
}

func (m *Monitor) installMetrics(meter metric.Meter) {
	m.watermarkGauge = utils.Must(telemetry.GetGaugeFloat(meter, telemetry.NodeHugepagesWatermarkGaugeName))
	m.rateGauge = utils.Must(telemetry.GetGaugeFloat(meter, telemetry.NodeHugepagesGrowthRateBytesGauge))
	m.tteGauge = utils.Must(telemetry.GetGaugeFloat(meter, telemetry.NodeHugepagesTTESecondsGaugeName))

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveFloat64(m.watermarkGauge, m.Watermark())
		o.ObserveFloat64(m.rateGauge, m.PredictedRateBytesPerSec())
		o.ObserveFloat64(m.tteGauge, math.Float64frombits(m.publishedTTE.Load()))
		return nil
	}, m.watermarkGauge, m.rateGauge, m.tteGauge)
	if err != nil {
		return
	}
	m.registeredCallbk = reg
}
