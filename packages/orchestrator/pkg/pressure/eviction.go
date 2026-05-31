package pressure

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/procstats"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Eviction uses SIGKILL — snapshotting needs hugepages, the resource we're out of.
type Eviction struct {
	sandboxes   *sandbox.Map
	hostMetrics hugepageReader
	stopFn      StopFunc

	triggerFrac float64
	stopFrac    float64
	hotzoneFrac float64

	postBatchCooldown time.Duration
	interKillDelay    time.Duration
	maxKillsPerRound  int
	stagnantWindow    time.Duration
	batchBudget       time.Duration
	stopFnTimeout     time.Duration
	refreshTimeout    time.Duration

	tickIdle    time.Duration
	tickHotzone time.Duration
	tickPanic   time.Duration
	panicFrac   float64

	maxScanConcurrency int

	topN atomic.Pointer[[]victimEntry]
	// actualPrivateSum: sum of Private_Hugetlb — the bytes SIGKILL can free.
	// Shared_Hugetlb (memfd) survives the kill because peers of the same
	// buildID hold it. Used by Monitor's cushion and the deficit guard.
	actualPrivateSum atomic.Uint64

	killCount atomic.Uint64

	mu           sync.Mutex
	lastActionAt time.Time

	readHugetlbFn func(pid int) (procstats.HugetlbStats, error)
	now           func() time.Time
	sleep         func(time.Duration)
	refreshFn     func(ctx context.Context)

	closeOnce sync.Once
	closed    chan struct{}

	// wake is buffered 1 — redundant signals coalesce. Lets the sampler / UFFD
	// ENOMEM preempt the tick timer instead of waiting up to tickHotzone.
	wake chan struct{}

	evictTotal       metric.Int64Counter
	evictDuration    metric.Int64Histogram
	evictRoundTotal  metric.Int64Counter
	cooldownBypass   metric.Int64Counter
}

type victimEntry struct {
	sbx          *sandbox.Sandbox
	pid          int
	privateBytes uint64
	sharedBytes  uint64
}

type EvictionOptions struct {
	TriggerFrac        float64
	StopFrac           float64
	HotzoneFrac        float64
	PanicFrac          float64
	PostBatchCooldown  time.Duration
	InterKillDelay     time.Duration
	MaxKillsPerRound   int
	StagnantWindow     time.Duration
	BatchBudget        time.Duration
	StopFnTimeout      time.Duration
	RefreshTimeout     time.Duration
	TickIdle           time.Duration
	TickHotzone        time.Duration
	TickPanic          time.Duration
	MaxScanConcurrency int
}

func NewEviction(
	sandboxes *sandbox.Map,
	hostMetrics hugepageReader,
	stopFn StopFunc,
	meter metric.Meter,
	opts EvictionOptions,
) *Eviction {
	if opts.TriggerFrac <= 0 {
		opts.TriggerFrac = evictTriggerFrac
	}
	if opts.StopFrac <= 0 {
		opts.StopFrac = evictStopFrac
	}
	if opts.HotzoneFrac <= 0 {
		opts.HotzoneFrac = evictHotzoneFrac
	}
	if opts.PostBatchCooldown <= 0 {
		opts.PostBatchCooldown = postBatchCooldown
	}
	if opts.InterKillDelay <= 0 {
		opts.InterKillDelay = interKillDelay
	}
	if opts.MaxKillsPerRound <= 0 {
		opts.MaxKillsPerRound = maxKillsPerRound
	}
	if opts.StagnantWindow <= 0 {
		opts.StagnantWindow = stagnantWindow
	}
	if opts.BatchBudget <= 0 {
		opts.BatchBudget = evictBatchBudget
	}
	if opts.StopFnTimeout <= 0 {
		opts.StopFnTimeout = stopFnTimeout
	}
	if opts.RefreshTimeout <= 0 {
		opts.RefreshTimeout = refreshTopNTimeout
	}
	if opts.TickIdle <= 0 {
		opts.TickIdle = evictTickIdle
	}
	if opts.TickHotzone <= 0 {
		opts.TickHotzone = evictTickHotzone
	}
	if opts.TickPanic <= 0 {
		opts.TickPanic = evictTickPanic
	}
	if opts.PanicFrac <= 0 {
		opts.PanicFrac = evictPanicFrac
	}
	if opts.MaxScanConcurrency <= 0 {
		opts.MaxScanConcurrency = 16
	}

	e := &Eviction{
		sandboxes:             sandboxes,
		hostMetrics:           hostMetrics,
		stopFn:                stopFn,
		triggerFrac:           opts.TriggerFrac,
		stopFrac:              opts.StopFrac,
		hotzoneFrac:           opts.HotzoneFrac,
		postBatchCooldown:     opts.PostBatchCooldown,
		interKillDelay:        opts.InterKillDelay,
		maxKillsPerRound:      opts.MaxKillsPerRound,
		stagnantWindow:        opts.StagnantWindow,
		batchBudget:           opts.BatchBudget,
		stopFnTimeout:         opts.StopFnTimeout,
		refreshTimeout:        opts.RefreshTimeout,
		tickIdle:              opts.TickIdle,
		tickHotzone:           opts.TickHotzone,
		tickPanic:             opts.TickPanic,
		panicFrac:             opts.PanicFrac,
		maxScanConcurrency:    opts.MaxScanConcurrency,
		readHugetlbFn:         procstats.ReadHugetlbStats,
		now:                   time.Now,
		sleep:                 time.Sleep,
		closed:                make(chan struct{}),
		wake:                  make(chan struct{}, 1),
	}
	e.refreshFn = e.refreshTopN
	if meter != nil {
		e.evictTotal = utils.Must(telemetry.GetCounter(meter, telemetry.NodePressureEvictTotal))
		e.evictDuration = utils.Must(telemetry.GetHistogram(meter, telemetry.NodePressureEvictDurationMs))
		e.evictRoundTotal = utils.Must(telemetry.GetCounter(meter, telemetry.NodePressureEvictRoundTotal))
		e.cooldownBypass = utils.Must(telemetry.GetCounter(meter, telemetry.NodePressureCooldownBypassTotal))
	}
	return e
}

func (e *Eviction) Start(ctx context.Context) error {
	timer := time.NewTimer(e.tickIdle)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-e.closed:
			return nil
		case <-e.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			next := e.tick(ctx)
			timer.Reset(next)
		case <-timer.C:
			next := e.tick(ctx)
			timer.Reset(next)
		}
	}
}

// Wake triggers an immediate tick. Non-blocking; redundant calls coalesce.
func (e *Eviction) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Eviction) Close(_ context.Context) error {
	e.closeOnce.Do(func() { close(e.closed) })
	return nil
}

func (e *Eviction) tick(ctx context.Context) time.Duration {
	e.refreshFn(ctx)

	hp, err := e.hostMetrics.GetHugepageMetrics()
	if err != nil || hp == nil || hp.TotalBytes == 0 {
		return e.tickIdle
	}
	usage := poolUsage(hp)

	nextInterval := e.tickIdle
	switch {
	case usage >= e.panicFrac:
		nextInterval = e.tickPanic
	case usage >= e.hotzoneFrac:
		nextInterval = e.tickHotzone
	}

	if usage < e.triggerFrac {
		return nextInterval
	}

	e.mu.Lock()
	elapsed := e.now().Sub(e.lastActionAt)
	inCooldown := !e.lastActionAt.IsZero() && elapsed < e.postBatchCooldown
	e.mu.Unlock()
	// Cooldown lets kernel reclaim land before the next round, but at
	// panicFrac innocent sandboxes are already ENOMEM-stalling — bypass.
	if inCooldown && usage < e.panicFrac {
		return nextInterval
	}
	if inCooldown {
		logger.L().Warn(ctx, "pressure eviction: bypassing cooldown — usage at panicFrac",
			zap.Float64("usage", usage),
			zap.Float64("panic_frac", e.panicFrac),
			zap.Duration("cooldown_remaining", e.postBatchCooldown-elapsed),
		)
		if e.cooldownBypass != nil {
			e.cooldownBypass.Add(ctx, 1)
		}
	}

	killed, outcome := e.evictBatch(ctx, hp)

	// deficit_skip / topn_empty also trigger cooldown so leak-alert Errors
	// fire at 5s cadence, not 50ms (tickPanic) under stuck conditions.
	if killed > 0 || outcome == "deficit_skip" || outcome == "topn_empty" {
		e.mu.Lock()
		e.lastActionAt = e.now()
		e.mu.Unlock()
	}

	return nextInterval
}

// evictBatch runs one rescue round. Returns (kills_attempted, outcome).
// outcome ∈ drained|deficit_skip|topn_empty|stagnant|budget_exhausted|max_kills|meminfo_error.
func (e *Eviction) evictBatch(ctx context.Context, hp *metrics.HugepageMetrics) (int, string) {
	outcome := "drained"
	defer func() {
		if e.evictRoundTotal != nil {
			e.evictRoundTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
		}
	}()

	logger.L().Warn(ctx, "pressure eviction: entering [Evicting]",
		zap.Float64("usage", poolUsage(hp)),
		zap.Float64("trigger_frac", e.triggerFrac),
		zap.Float64("stop_frac", e.stopFrac),
	)

	// Deficit guard: sum of freeable Private bytes < gap-to-stopFrac means the
	// deficit lives outside sandboxes (kernel page tables, hugetlbfs reserve,
	// or the memfd pool itself). Killing every sandbox won't help.
	stopFracBytes := uint64(e.stopFrac * float64(hp.TotalBytes))
	if hp.FreeBytes < stopFracBytes {
		need := stopFracBytes - hp.FreeBytes
		var topNPrivateSum uint64
		if top := e.topN.Load(); top != nil {
			for _, v := range *top {
				topNPrivateSum += v.privateBytes
			}
		}
		if topNPrivateSum < need {
			logger.L().Error(ctx, "pressure eviction: Top-N freeable < deficit — non-sandbox holder, skipping kill",
				zap.Uint64("topn_private_sum_bytes", topNPrivateSum),
				zap.Uint64("deficit_bytes", need),
				zap.Uint64("free_bytes", hp.FreeBytes),
				zap.Uint64("total_bytes", hp.TotalBytes),
			)
			outcome = "deficit_skip"
			return 0, outcome
		}
	}

	// Time-window stagnant tracker rides out async kernel reclaim: any rise in
	// free resets the timer; no rise within stagnantWindow → reclaim is broken.
	bestFree := hp.FreeBytes
	stagnantSince := e.now()
	killed := 0
	deadline := e.now().Add(e.batchBudget)

	for killed < e.maxKillsPerRound {
		// Don't start a kill that would overshoot deadline — would blow past
		// UFFD stallOnEnomem budget.
		if e.now().Add(e.stopFnTimeout).After(deadline) {
			logger.L().Error(ctx, "pressure eviction: batch budget exhausted — bailing out",
				zap.Duration("budget", e.batchBudget),
				zap.Int("kills_this_round", killed),
			)
			outcome = "budget_exhausted"
			return killed, outcome
		}
		top := e.topN.Load()
		if top == nil || len(*top) == 0 {
			curHp, rerr := e.hostMetrics.GetHugepageMetrics()
			if rerr == nil && curHp != nil && curHp.TotalBytes > 0 && poolUsage(curHp) >= e.stopFrac {
				logger.L().Error(ctx, "pressure eviction: Top-N empty but pool still ≥ stopFrac — possible hugetlb page-table leak",
					zap.Uint64("free_bytes", curHp.FreeBytes),
					zap.Uint64("total_bytes", curHp.TotalBytes),
				)
			}
			outcome = "topn_empty"
			return killed, outcome
		}
		v := (*top)[0]

		startedAt := e.now()
		logger.L().Warn(ctx, "pressure eviction: SIGKILL victim",
			zap.String("victim_sandbox_id", v.sbx.Runtime.SandboxID),
			zap.Int("victim_pid", v.pid),
			zap.Uint64("victim_private_hugetlb_bytes", v.privateBytes),
			zap.Uint64("victim_shared_hugetlb_bytes", v.sharedBytes),
			zap.Int("kill_index", killed),
		)
		stopErr := e.callStopFn(ctx, v.sbx)
		killed++
		e.killCount.Add(1)
		durationMs := e.now().Sub(startedAt).Milliseconds()
		killOutcome := "success"
		if stopErr != nil {
			killOutcome = "failure"
			logger.L().Error(ctx, "pressure eviction: stop call failed",
				zap.String("sandbox_id", v.sbx.Runtime.SandboxID),
				zap.Error(stopErr),
			)
		}
		if e.evictTotal != nil {
			e.evictTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", killOutcome)))
		}
		if e.evictDuration != nil {
			e.evictDuration.Record(ctx, durationMs, metric.WithAttributes(attribute.String("outcome", killOutcome)))
		}

		e.sleep(e.interKillDelay)

		curHp, rerr := e.hostMetrics.GetHugepageMetrics()
		if rerr != nil || curHp == nil || curHp.TotalBytes == 0 {
			outcome = "meminfo_error"
			return killed, outcome
		}

		if curHp.FreeBytes > bestFree {
			bestFree = curHp.FreeBytes
			stagnantSince = e.now()
		} else if e.now().Sub(stagnantSince) >= e.stagnantWindow {
			logger.L().Error(ctx, "pressure eviction: free has not risen within stagnant window — kernel reclaim path may be broken",
				zap.Duration("stagnant_window", e.stagnantWindow),
				zap.Uint64("free_bytes", curHp.FreeBytes),
				zap.Uint64("best_free_bytes", bestFree),
			)
			outcome = "stagnant"
			return killed, outcome
		}

		if poolUsage(curHp) < e.stopFrac {
			logger.L().Info(ctx, "pressure eviction: drained below stopFrac",
				zap.Float64("usage", poolUsage(curHp)),
				zap.Int("kills_this_round", killed),
			)
			return killed, outcome
		}

		// Skip the next refreshTopN (~refreshTimeout) if we'd land past deadline.
		if !e.now().Before(deadline) {
			outcome = "budget_exhausted"
			return killed, outcome
		}

		e.refreshFn(ctx)
	}

	logger.L().Error(ctx, "pressure eviction: hit maxKillsPerRound — bailing out",
		zap.Int("max_kills_per_round", e.maxKillsPerRound),
	)
	outcome = "max_kills"
	return killed, outcome
}

// callStopFn: detach from parent cancel so kills survive orchestrator
// shutdown; cap wall time so a hung stopFn can't wedge the rescue loop.
func (e *Eviction) callStopFn(ctx context.Context, sbx *sandbox.Sandbox) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.stopFnTimeout)
	defer cancel()
	return e.stopFn(stopCtx, sbx, map[string]any{
		"reason": EvictReason,
	})
}

func poolUsage(hp *metrics.HugepageMetrics) float64 {
	if hp == nil || hp.TotalBytes == 0 {
		return 0
	}
	return float64(hp.TotalBytes-hp.FreeBytes) / float64(hp.TotalBytes)
}

func (e *Eviction) refreshTopN(ctx context.Context) {
	items := e.sandboxes.Items()
	if len(items) == 0 {
		empty := []victimEntry{}
		e.topN.Store(&empty)
		e.actualPrivateSum.Store(0)
		return
	}

	sem := make(chan struct{}, e.maxScanConcurrency)
	var wg sync.WaitGroup
	out := make([]victimEntry, 0, len(items))
	var sum uint64
	var mu sync.Mutex

	for _, sbx := range items {
		wg.Add(1)
		go func(sbx *sandbox.Sandbox) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pid, err := sbx.Pid()
			if err != nil {
				return
			}
			stats, rerr := e.readHugetlbFn(pid)
			if rerr != nil {
				logger.L().Debug(ctx, "pressure eviction: failed to read hugetlb",
					zap.String("sandbox_id", sbx.Runtime.SandboxID),
					zap.Int("pid", pid),
					zap.Error(rerr),
				)
				return
			}
			// Skip Private==0: only Shared (memfd) bytes, which SIGKILL can't free.
			if stats.PrivateBytes == 0 {
				return
			}
			mu.Lock()
			out = append(out, victimEntry{
				sbx:          sbx,
				pid:          pid,
				privateBytes: stats.PrivateBytes,
				sharedBytes:  stats.SharedBytes,
			})
			sum += stats.PrivateBytes
			mu.Unlock()
		}(sbx)
	}

	// Bound the wait — a hung /proc read must not wedge eviction. Stragglers
	// keep writing to `out` after timeout, but we snapshot under the same
	// mutex they hold so the stored Top-N stays internally consistent.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	timer := time.NewTimer(e.refreshTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		logger.L().Warn(ctx, "pressure eviction: refreshTopN timeout — using partial results",
			zap.Duration("timeout", e.refreshTimeout),
			zap.Int("scanned_total", len(items)),
		)
	}

	mu.Lock()
	snapshot := make([]victimEntry, len(out))
	copy(snapshot, out)
	snapshotSum := sum
	mu.Unlock()

	sort.Slice(snapshot, func(i, j int) bool {
		if snapshot[i].privateBytes != snapshot[j].privateBytes {
			return snapshot[i].privateBytes > snapshot[j].privateBytes
		}
		return snapshot[i].sbx.Runtime.SandboxID < snapshot[j].sbx.Runtime.SandboxID
	})
	e.topN.Store(&snapshot)
	e.actualPrivateSum.Store(snapshotSum)
}

func (e *Eviction) ActualPrivateBytes() uint64 {
	return e.actualPrivateSum.Load()
}

func (e *Eviction) KillCount() uint64 {
	return e.killCount.Load()
}

func (e *Eviction) TopN() []victimEntry {
	p := e.topN.Load()
	if p == nil {
		return nil
	}
	return *p
}
