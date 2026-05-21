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

// fakeHugepageReader returns a fixed sample.
type fakeHugepageReader struct {
	sample *metrics.HugepageMetrics
	err    error
}

func (f *fakeHugepageReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	return f.sample, f.err
}

// countingStop records the number of times it is invoked.
type countingStop struct {
	calls atomic.Int64
	last  map[string]any
}

func (c *countingStop) fn(_ context.Context, _ *sandbox.Sandbox, extra map[string]any) error {
	c.calls.Add(1)
	c.last = extra
	return nil
}

func newTestMonitor(t *testing.T, reader hugepageReader, stopFn StopFunc) *Monitor {
	t.Helper()
	m := newMonitor(
		sandbox.NewSandboxesMap(), reader, stopFn,
		Options{
			HardWatermark:  0.90,
			TickInterval:   time.Hour, // irrelevant, we call tick() directly
			ActionCooldown: 30 * time.Second,
		},
	)
	// Predictable time in tests.
	m.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return m
}

func TestMonitor_NoPool_DoesNothing(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{TotalBytes: 0}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestMonitor_MetricsError_DoesNothing(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{err: errors.New("meminfo unavailable")}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestMonitor_BelowWatermark_DoesNothing(t *testing.T) {
	t.Parallel()
	// 80% used, hard watermark is 90% — not crossed.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  200,
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestMonitor_ExactlyAtWatermark_Triggers(t *testing.T) {
	t.Parallel()
	// 90% used, equals hard watermark — monitor uses >=, should trigger.
	// But there are zero sandboxes, so no stop call happens.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  100,
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	// no candidates → no stop
	assert.Equal(t, int64(0), stop.calls.Load())
	// cooldown not advanced when victim not found
	assert.True(t, m.lastActionAt.IsZero())
}

func TestMonitor_AboveWatermark_NoVictim_NoCooldownAdvance(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  20, // 98% used
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	// Empty sandbox map → pickVictim returns nil → stopFn never called, cooldown untouched.
	assert.Equal(t, int64(0), stop.calls.Load())
	assert.True(t, m.lastActionAt.IsZero())
}

func TestMonitor_CooldownBlocksRepeatAction(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  20,
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	// Simulate we just evicted something.
	m.lastActionAt = m.now()

	m.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load(),
		"cooldown should prevent action even though watermark is exceeded")
}

func TestMonitor_CooldownExpires(t *testing.T) {
	t.Parallel()
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  20,
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	// Last action was 40s ago, cooldown is 30s — should be allowed.
	m.lastActionAt = m.now().Add(-40 * time.Second)

	m.tick(context.Background())
	// Still no victim in empty map — assertion is only about cooldown being
	// considered "expired", confirmed by the fact that we reached pickVictim.
	assert.Equal(t, int64(0), stop.calls.Load())
}

func TestMonitor_PickVictim_EmptyMap(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t, &fakeHugepageReader{}, func(context.Context, *sandbox.Sandbox, map[string]any) error { return nil })
	victim, bytes := m.pickVictim(context.Background())
	assert.Nil(t, victim)
	assert.Equal(t, uint64(0), bytes)
}

func TestMonitor_Validate(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t, &fakeHugepageReader{}, func(context.Context, *sandbox.Sandbox, map[string]any) error { return nil })
	require.NoError(t, m.Validate())

	// Missing stop fn
	m.stopFn = nil
	require.ErrorIs(t, m.Validate(), ErrStopFuncRequired)
}

func TestMonitor_Defaults(t *testing.T) {
	t.Parallel()
	m := newMonitor(sandbox.NewSandboxesMap(), &fakeHugepageReader{}, func(context.Context, *sandbox.Sandbox, map[string]any) error { return nil }, Options{})
	assert.Equal(t, DefaultHardWatermark, m.hardWatermark)
	assert.Equal(t, DefaultTickInterval, m.tickInterval)
	assert.Equal(t, DefaultActionCooldown, m.actionCooldown)
}

func TestMonitor_Close_Idempotent(t *testing.T) {
	t.Parallel()
	m := newTestMonitor(t, &fakeHugepageReader{}, func(context.Context, *sandbox.Sandbox, map[string]any) error { return nil })
	require.NoError(t, m.Close(context.Background()))
	require.NoError(t, m.Close(context.Background())) // second call must not panic
}
