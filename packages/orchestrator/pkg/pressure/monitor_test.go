package pressure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// fakeHugepageReader returns a fixed sample. Shared with eviction_test.go.
type fakeHugepageReader struct {
	sample *metrics.HugepageMetrics
	err    error
}

func (f *fakeHugepageReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	return f.sample, f.err
}

// countingStop records the number of times stopFn is invoked. Shared with
// eviction_test.go.
type countingStop struct {
	calls atomic.Int64
	last  map[string]any
}

func (c *countingStop) fn(_ context.Context, _ *sandbox.Sandbox, extra map[string]any) error {
	c.calls.Add(1)
	c.last = extra
	return nil
}

func newTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	stopFn := func(context.Context, *sandbox.Sandbox, map[string]any) error { return nil }
	hostMetrics := metrics.NewHostMetrics()
	m := NewMonitor(sandbox.NewSandboxesMap(), hostMetrics, stopFn, Options{})
	m.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return m
}

func TestMonitor_Validate(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	require.NoError(t, m.Validate())

	m.stopFn = nil
	require.ErrorIs(t, m.Validate(), ErrStopFuncRequired)
}

func TestMonitor_Defaults(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	assert.Equal(t, sampleIdle, m.opts.SampleIdle)
	assert.Equal(t, sampleWarm, m.opts.SampleWarm)
	assert.Equal(t, sampleHot, m.opts.SampleHot)
	assert.Equal(t, samplePanic, m.opts.SamplePanic)
	assert.Equal(t, watermarkRecomputeInterval, m.opts.WatermarkRecomputeInterval)
	assert.Equal(t, evictTriggerFrac, m.opts.EvictTriggerFrac)
	assert.Equal(t, evictHotzoneFrac, m.opts.EvictHotzoneFrac)
	assert.Equal(t, postBatchCooldown, m.opts.EvictPostBatchCooldown)
	assert.Equal(t, evictStopFrac, m.opts.EvictStopFrac)
	assert.Equal(t, interKillDelay, m.opts.EvictInterKillDelay)
	assert.Equal(t, maxKillsPerRound, m.opts.EvictMaxKillsPerRound)
	assert.Equal(t, stagnantWindow, m.opts.EvictStagnantWindow)
}

func TestMonitor_InitialPublishedWatermark(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	// Before any tick, watermark equals tMax (admission fully open).
	assert.InDelta(t, tMax, m.Watermark(), 0.001)
	assert.Equal(t, 0.0, m.PredictedRateBytesPerSec())
}

func TestMonitor_Close_Idempotent(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	require.NoError(t, m.Close(context.Background()))
	require.NoError(t, m.Close(context.Background()))
}

func TestMonitor_Sample_AdaptiveCadence(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)

	// Inject a fake pool reader so we don't touch /proc/meminfo.
	var total, free uint64
	m.readPoolFn = func() (uint64, uint64, error) { return total, free, nil }

	// Idle: 50% used → SampleIdle.
	total, free = 1000, 500
	assert.Equal(t, m.opts.SampleIdle, m.sample(context.Background()))

	// Warm: 75% used (>= 0.70) → SampleWarm.
	total, free = 1000, 250
	assert.Equal(t, m.opts.SampleWarm, m.sample(context.Background()))

	// Hot: 88% used (>= 0.85) → SampleHot.
	total, free = 1000, 120
	assert.Equal(t, m.opts.SampleHot, m.sample(context.Background()))

	// Panic: 97% used (>= 0.95) → SamplePanic.
	total, free = 1000, 30
	assert.Equal(t, m.opts.SamplePanic, m.sample(context.Background()))
}

func TestMonitor_Sample_NoPool(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	m.readPoolFn = func() (uint64, uint64, error) { return 0, 0, nil }
	assert.Equal(t, m.opts.SampleIdle, m.sample(context.Background()))
}

func TestMonitor_Sample_ReadError(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)
	m.readPoolFn = func() (uint64, uint64, error) { return 0, 0, errors.New("kaboom") }
	assert.Equal(t, m.opts.SampleIdle, m.sample(context.Background()))
	assert.False(t, m.hasLastSample, "no last-sample state when read fails")
}

func TestMonitor_Sample_FeedsRateEstimator(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t)

	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time { return now }

	step := 0
	m.readPoolFn = func() (uint64, uint64, error) {
		// First call: free=1000. Second: free=500 (1s later → rate 500 B/s).
		switch step {
		case 0:
			step++
			return 2000, 1000, nil
		default:
			return 2000, 500, nil
		}
	}

	// First sample seeds state; rate estimator gets nothing.
	m.sample(context.Background())
	assert.Equal(t, 0.0, m.rate.Predict(), "first sample only seeds state")

	// Advance time and sample again.
	now = now.Add(1 * time.Second)
	m.sample(context.Background())
	assert.Greater(t, m.rate.Predict(), 0.0, "rate estimator must register the drop")
}
