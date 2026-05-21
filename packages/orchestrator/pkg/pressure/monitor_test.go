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

type fakeHugepageReader struct {
	sample *metrics.HugepageMetrics
	err    error
}

func (f *fakeHugepageReader) GetHugepageMetrics() (*metrics.HugepageMetrics, error) {
	return f.sample, f.err
}

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
			TickInterval:   time.Hour,
			ActionCooldown: 30 * time.Second,
		},
	)
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
	// 80% used, watermark 90%.
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
	// 90% used, exactly at watermark; no sandboxes → no stop, cooldown not advanced.
	reader := &fakeHugepageReader{sample: &metrics.HugepageMetrics{
		TotalBytes: 1000,
		FreeBytes:  100,
	}}
	stop := &countingStop{}
	m := newTestMonitor(t, reader, stop.fn)

	m.tick(context.Background())
	assert.Equal(t, int64(0), stop.calls.Load())
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

	// 40s ago > 30s cooldown.
	m.lastActionAt = m.now().Add(-40 * time.Second)

	m.tick(context.Background())
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
	require.NoError(t, m.Close(context.Background()))
}
