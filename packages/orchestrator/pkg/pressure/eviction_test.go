package pressure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func newTestEviction(reader hugepageReader, stop StopFunc) *Eviction {
	e := NewEviction(
		sandbox.NewSandboxesMap(), reader, stop, nil,
		EvictionOptions{
			TriggerFrac:       0.99,
			StopFrac:          0.95,
			HotzoneFrac:       0.90,
			PostBatchCooldown: 5 * time.Second,
			InterKillDelay:    time.Millisecond,
			MaxKillsPerRound:  20,
			StagnantWindow:    3 * time.Millisecond,
			TickIdle:          time.Hour,
			TickHotzone:       time.Hour,
		},
	)
	e.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	e.sleep = func(time.Duration) {}
	return e
}

func TestEviction_NoPool_NoAction(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	e := newTestEviction(&fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 0}}, stop.fn)
	next := e.tick(context.Background())
	assert.Equal(t, time.Hour, next, "no pool → idle interval")
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestEviction_BelowTrigger_HotzonePicksHotzoneInterval(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 80}}
	e := NewEviction(sandbox.NewSandboxesMap(), reader, stop.fn, nil, EvictionOptions{
		TriggerFrac:       0.99,
		StopFrac:          0.95,
		HotzoneFrac:       0.90,
		PanicFrac:         0.95,
		PostBatchCooldown: 5 * time.Second,
		InterKillDelay:    time.Millisecond,
		TickIdle:          time.Second,
		TickHotzone:       200 * time.Millisecond,
		TickPanic:         50 * time.Millisecond,
	})
	e.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	e.sleep = func(time.Duration) {}

	next := e.tick(context.Background())
	assert.Equal(t, 200*time.Millisecond, next)
	assert.Equal(t, int64(0), stop.calls.Load(), "below trigger → no eviction")
}

func TestEviction_PanicZonePicksPanicInterval(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 40}}
	e := NewEviction(sandbox.NewSandboxesMap(), reader, stop.fn, nil, EvictionOptions{
		TriggerFrac:       0.99,
		StopFrac:          0.95,
		HotzoneFrac:       0.90,
		PanicFrac:         0.95,
		PostBatchCooldown: 5 * time.Second,
		InterKillDelay:    time.Millisecond,
		TickIdle:          time.Second,
		TickHotzone:       200 * time.Millisecond,
		TickPanic:         50 * time.Millisecond,
	})
	e.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	e.sleep = func(time.Duration) {}

	next := e.tick(context.Background())
	assert.Equal(t, 50*time.Millisecond, next, "panic zone should pick tickPanic")
	assert.Equal(t, int64(0), stop.calls.Load(), "below trigger → no eviction")
}

func TestEviction_AboveTrigger_NoVictims(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	e := newTestEviction(reader, stop.fn)
	e.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load(), "no sandboxes → no victim → no eviction")
	// topn_empty (no sandboxes) sets cooldown to throttle the leak-alert log
	// from firing every tickPanic (50 ms). One Error log per cooldown is enough.
	assert.False(t, e.lastActionAt.IsZero(), "topn_empty must engage cooldown to throttle log spam")
}

func TestEviction_Cooldown(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	e := newTestEviction(reader, stop.fn)
	e.lastActionAt = e.now()
	e.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestEviction_StopFailureCountsAsKill(t *testing.T) {
	t.Parallel()
	failing := func(_ context.Context, _ *sandbox.Sandbox, _ map[string]any) error {
		return errors.New("kaboom")
	}
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	e := newTestEviction(reader, failing)
	fake := []victimEntry{{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 123, privateBytes: 999}}
	calls := atomic.Int64{}
	e.stopFn = func(ctx context.Context, sbx *sandbox.Sandbox, extra map[string]any) error {
		calls.Add(1)
		return failing(ctx, sbx, extra)
	}
	e.topN.Store(&fake)
	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	assert.Equal(t, int64(1), calls.Load(), "stopFn should be invoked even on later failure")
	assert.Equal(t, 1, killed, "failed kill still counts so cooldown engages")
}

// steppingReader returns steps[i] in sequence; sticks on the last entry.
type steppingReader struct {
	mu    sync.Mutex
	steps []*metrics.HugepageMetrics
	idx   int
}

func (s *steppingReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.steps[s.idx]
	if s.idx < len(s.steps)-1 {
		s.idx++
	}
	return cur, nil
}

func TestEviction_BatchDrainsToStopFrac(t *testing.T) {
	t.Parallel()
	reader := &steppingReader{steps: []*metrics.HugepageMetrics{
		{TotalBytes: 1000, FreeBytes: 80},
	}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 2, privateBytes: 888},
	})

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, 1, killed, "one kill is enough to drop below stopFrac")
	assert.Equal(t, int64(1), stop.calls.Load())
}

func TestEviction_BatchHitsMaxKillsPerRound(t *testing.T) {
	t.Parallel()
	// Free rises just enough each kill to dodge the stagnant guard but never
	// cross stopFrac, so the loop runs to maxKillsPerRound.
	steps := []*metrics.HugepageMetrics{{TotalBytes: 1000, FreeBytes: 10}}
	for i := 1; i <= 25; i++ {
		steps = append(steps, &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10 + uint64(i)})
	}
	reader := &steppingReader{steps: steps}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	victims := make([]victimEntry, 30)
	for i := range victims {
		victims[i] = victimEntry{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: i, privateBytes: 100}
	}
	e.topN.Store(&victims)

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, 20, killed, "should cap at maxKillsPerRound")
	assert.Equal(t, int64(20), stop.calls.Load())
}

func TestEviction_BatchDetectsStagnantFree(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	// Advancing clock: each interKillDelay sleep moves time forward, so the
	// stagnant window elapses after enough non-rising kills.
	clock := atomic.Int64{}
	clock.Store(time.Unix(1_700_000_000, 0).UnixNano())
	e.now = func() time.Time { return time.Unix(0, clock.Load()) }
	e.sleep = func(d time.Duration) { clock.Add(int64(d)) }

	victims := make([]victimEntry, 10)
	for i := range victims {
		victims[i] = victimEntry{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: i, privateBytes: 100}
	}
	e.topN.Store(&victims)

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	// stagnantWindow=3ms, interKillDelay=1ms — bail when accumulated stagnant
	// time hits the window. Exact count is timing-dependent, so just assert
	// we bailed before exhausting victims.
	assert.Greater(t, killed, 0, "should kill at least once before stagnant trips")
	assert.Less(t, killed, 10, "should bail before exhausting victims")
}

func TestEviction_BatchSkipsWhenTopNSumBelowDeficit(t *testing.T) {
	t.Parallel()
	// deficit=940 (need to drain to stopFrac=0.95), Top-N sum=200 → skip.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 100},
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 2, privateBytes: 100},
	})

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, 0, killed, "deficit guard should skip kill when Top-N can't cover the gap")
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestEviction_TickEngagesCooldownOnTopnEmpty(t *testing.T) {
	t.Parallel()
	// Pool above trigger but Top-N empty: cooldown engages so the leak-alert
	// Error log fires every postBatchCooldown (5 s) instead of every
	// tickPanic (50 ms) → 100× quieter under stuck conditions.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)

	e.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
	assert.False(t, e.lastActionAt.IsZero(), "topn_empty must engage cooldown")
}

func TestEviction_TickEngagesCooldownOnDeficitSkip(t *testing.T) {
	t.Parallel()
	// Same intent as topn_empty: deficit_skip means we tried but couldn't
	// help. Without cooldown the Error log would fire every tickPanic.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	// Top-N sum (200) << deficit (940 to reach stopFrac=0.95).
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 100},
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 2, privateBytes: 100},
	})

	e.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load(), "deficit_skip → no kill")
	assert.False(t, e.lastActionAt.IsZero(), "deficit_skip must engage cooldown to throttle log spam")
}

func TestEviction_TickBypassesCooldownAtPanicFrac(t *testing.T) {
	t.Parallel()
	// Free=10/Total=1000 → usage 99% > panicFrac (0.95). Cooldown is active
	// (lastActionAt set to now), so without the bypass the tick would skip.
	steps := []*metrics.HugepageMetrics{
		{TotalBytes: 1000, FreeBytes: 10},
		{TotalBytes: 1000, FreeBytes: 60}, // post-kill drain below stopFrac
	}
	reader := &steppingReader{steps: steps}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})
	e.lastActionAt = e.now() // simulate "just killed something" → cooldown active

	e.tick(context.Background())
	assert.Equal(t, int64(1), stop.calls.Load(), "panicFrac usage should bypass cooldown")
}

func TestEviction_CooldownBypass_IncrementsCounter(t *testing.T) {
	t.Parallel()
	steps := []*metrics.HugepageMetrics{
		{TotalBytes: 1000, FreeBytes: 10},
		{TotalBytes: 1000, FreeBytes: 60},
	}
	reader := &steppingReader{steps: steps}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})
	e.lastActionAt = e.now()

	e.tick(context.Background())
	assert.Equal(t, int64(1), counterValue(t, mr, telemetry.NodePressureCooldownBypassTotal),
		"cooldown bypass at panicFrac should increment counter")
}

func TestEviction_TickHonorsCooldownBelowPanic(t *testing.T) {
	t.Parallel()
	// Free=20/Total=1000 → usage 98% > triggerFrac=0.99? No, 98% < 99%, so
	// trigger doesn't fire either. Use Free=5 → 99.5% > trigger but Set
	// panicFrac higher to ensure usage < panic.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	stop := &countingStop{}
	e := newTestEviction(reader, stop.fn)
	e.refreshFn = func(context.Context) {}
	e.panicFrac = 0.999 // bump panic above current usage (99.5%)
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})
	e.lastActionAt = e.now()

	e.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load(), "below panicFrac → cooldown still in effect")
}

func TestEviction_RefreshTopN_EmptyMap(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	e := newTestEviction(&fakeHugepageReader{}, stop.fn)
	e.refreshTopN(context.Background())
	assert.Empty(t, e.TopN())
}

func TestEviction_Close_Idempotent(t *testing.T) {
	t.Parallel()
	stop := &countingStop{}
	e := newTestEviction(&fakeHugepageReader{}, stop.fn)
	assert.NoError(t, e.Close(context.Background()))
	assert.NoError(t, e.Close(context.Background()))
}

// counterValue returns the total of a no-attribute Int64 counter from the
// reader. Returns 0 if the counter has not been recorded yet.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name telemetry.CounterType) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func roundOutcomes(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.NodePressureEvictRoundTotal) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				v, _ := dp.Attributes.Value("outcome")
				out[v.AsString()] = dp.Value
			}
		}
	}
	return out
}

func withMeter(t *testing.T, reader hugepageReader, stop StopFunc, opts EvictionOptions) (*Eviction, *sdkmetric.ManualReader) {
	t.Helper()
	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	meter := mp.Meter("pressure-test")
	e := NewEviction(sandbox.NewSandboxesMap(), reader, stop, meter, opts)
	e.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	e.sleep = func(time.Duration) {}
	return e, mr
}

func baseOpts() EvictionOptions {
	return EvictionOptions{
		TriggerFrac:       0.99,
		StopFrac:          0.95,
		HotzoneFrac:       0.90,
		PostBatchCooldown: 5 * time.Second,
		InterKillDelay:    time.Millisecond,
		MaxKillsPerRound:  20,
		StagnantWindow:    3 * time.Millisecond,
		TickIdle:          time.Hour,
		TickHotzone:       time.Hour,
	}
}

func TestEviction_RoundOutcome_Drained(t *testing.T) {
	t.Parallel()
	reader := &steppingReader{steps: []*metrics.HugepageMetrics{
		{TotalBytes: 1000, FreeBytes: 80},
	}}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, map[string]int64{"drained": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_DeficitSkip(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10}}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 100},
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 2, privateBytes: 100},
	})

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, map[string]int64{"deficit_skip": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_TopNEmpty(t *testing.T) {
	t.Parallel()
	// free >= stopFracBytes bypasses the deficit guard, so an empty Top-N
	// surfaces as topn_empty rather than deficit_skip.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 960}}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 960})
	assert.Equal(t, map[string]int64{"topn_empty": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_Stagnant(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	clock := atomic.Int64{}
	clock.Store(time.Unix(1_700_000_000, 0).UnixNano())
	e.now = func() time.Time { return time.Unix(0, clock.Load()) }
	e.sleep = func(d time.Duration) { clock.Add(int64(d)) }
	victims := make([]victimEntry, 10)
	for i := range victims {
		victims[i] = victimEntry{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: i, privateBytes: 100}
	}
	e.topN.Store(&victims)

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	assert.Equal(t, map[string]int64{"stagnant": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_MaxKills(t *testing.T) {
	t.Parallel()
	steps := []*metrics.HugepageMetrics{{TotalBytes: 1000, FreeBytes: 10}}
	for i := 1; i <= 25; i++ {
		steps = append(steps, &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10 + uint64(i)})
	}
	reader := &steppingReader{steps: steps}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	victims := make([]victimEntry, 30)
	for i := range victims {
		victims[i] = victimEntry{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: i, privateBytes: 100}
	}
	e.topN.Store(&victims)

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, map[string]int64{"max_kills": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_BudgetExhausted(t *testing.T) {
	t.Parallel()
	// Free creeps up just enough to dodge stagnant guard but never crosses
	// stopFrac, so without the budget the loop would run to maxKillsPerRound.
	steps := []*metrics.HugepageMetrics{{TotalBytes: 1000, FreeBytes: 10}}
	for i := 1; i <= 25; i++ {
		steps = append(steps, &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10 + uint64(i)})
	}
	reader := &steppingReader{steps: steps}
	stop := &countingStop{}
	opts := baseOpts()
	opts.BatchBudget = 10 * time.Millisecond
	opts.InterKillDelay = 2 * time.Millisecond
	opts.StopFnTimeout = 1 * time.Millisecond
	e, mr := withMeter(t, reader, stop.fn, opts)
	e.refreshFn = func(context.Context) {}
	victims := make([]victimEntry, 30)
	for i := range victims {
		victims[i] = victimEntry{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: i, privateBytes: 100}
	}
	e.topN.Store(&victims)

	// Fake clock that advances on each sleep — drives the deadline check.
	clock := atomic.Int64{}
	clock.Store(time.Unix(1_700_000_000, 0).UnixNano())
	e.now = func() time.Time { return time.Unix(0, clock.Load()) }
	e.sleep = func(d time.Duration) { clock.Add(int64(d)) }

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	assert.Equal(t, map[string]int64{"budget_exhausted": 1}, roundOutcomes(t, mr))
	assert.Greater(t, killed, 0, "should kill at least once before budget trips")
	assert.Less(t, killed, opts.MaxKillsPerRound, "must exit before maxKillsPerRound")
}

// Headroom guard: budget shorter than stopFnTimeout → exit before any kill.
// Without the guard, we'd start a kill that overshoots the budget by stopFn's
// own timeout, defeating the UFFD-fit purpose of the budget.
func TestEviction_BudgetHeadroomBlocksKillStart(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5}}
	stop := &countingStop{}
	opts := baseOpts()
	opts.BatchBudget = 1 * time.Millisecond
	opts.StopFnTimeout = 10 * time.Millisecond
	e, mr := withMeter(t, reader, stop.fn, opts)
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	assert.Equal(t, 0, killed, "headroom guard should block first kill")
	assert.Equal(t, int64(0), stop.calls.Load())
	assert.Equal(t, map[string]int64{"budget_exhausted": 1}, roundOutcomes(t, mr))
}

func TestEviction_RoundOutcome_MeminfoError(t *testing.T) {
	t.Parallel()
	reader := &errOnNthReader{
		samples: []*metrics.HugepageMetrics{nil},
		errs:    []error{errors.New("meminfo unavailable")},
	}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})

	e.evictBatch(context.Background(), &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 5})
	assert.Equal(t, map[string]int64{"meminfo_error": 1}, roundOutcomes(t, mr))
}

// Eviction must keep working through orchestrator shutdown — that's when
// rescue matters most. Verify callStopFn's WithoutCancel actually shields
// stopFn from a cancelled parent.
func TestEviction_BatchDetachesFromParentCancel(t *testing.T) {
	t.Parallel()
	var (
		mu              sync.Mutex
		invocationCount int
		ctxErrSeen      error
		hadDeadline     bool
	)
	stopFn := func(stopCtx context.Context, _ *sandbox.Sandbox, _ map[string]any) error {
		mu.Lock()
		defer mu.Unlock()
		invocationCount++
		ctxErrSeen = stopCtx.Err()
		_, hadDeadline = stopCtx.Deadline()
		return nil
	}
	reader := &steppingReader{steps: []*metrics.HugepageMetrics{
		{TotalBytes: 1000, FreeBytes: 80},
	}}
	e := newTestEviction(reader, stopFn)
	e.refreshFn = func(context.Context) {}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
	})

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	killed, _ := e.evictBatch(parent, &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, killed, "stopFn should run despite cancelled parent")
	assert.Equal(t, 1, invocationCount)
	assert.NoError(t, ctxErrSeen, "WithoutCancel must shield stopFn from parent cancellation")
	assert.True(t, hadDeadline, "stopFn ctx must carry the safety timeout")
}

// If the just-killed sandbox was the last hugepage holder, refreshFn returns
// an empty Top-N. The next iteration must exit as topn_empty without panicking
// on an empty slice.
func TestEviction_TopNFlapsToEmptyMidLoop(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10}}
	stop := &countingStop{}
	e, mr := withMeter(t, reader, stop.fn, baseOpts())

	flapped := atomic.Bool{}
	e.refreshFn = func(context.Context) {
		if flapped.CompareAndSwap(false, true) {
			empty := []victimEntry{}
			e.topN.Store(&empty)
		}
	}
	e.topN.Store(&[]victimEntry{
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 1, privateBytes: 999},
		{sbx: &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, pid: 2, privateBytes: 999},
	})

	killed, _ := e.evictBatch(context.Background(),&metrics.HugepageMetrics{TotalBytes: 1000, FreeBytes: 10})
	assert.Equal(t, 1, killed, "one kill before Top-N flapped to empty")
	assert.Equal(t, map[string]int64{"topn_empty": 1}, roundOutcomes(t, mr))
}

type errOnNthReader struct {
	mu      sync.Mutex
	samples []*metrics.HugepageMetrics
	errs    []error
	idx     int
}

func (r *errOnNthReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, e := r.samples[r.idx], r.errs[r.idx]
	if r.idx < len(r.samples)-1 {
		r.idx++
	}
	return s, e
}
