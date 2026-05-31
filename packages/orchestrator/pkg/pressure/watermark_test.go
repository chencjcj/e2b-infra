package pressure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputeTarget_NoPool(t *testing.T) {
	t.Parallel()
	// No hugepage pool → watermark inert (1.0, gate always passes).
	assert.Equal(t, 1.0, ComputeTarget(0, 0, 0, 0))
}

func TestComputeTarget_EmptyNode(t *testing.T) {
	t.Parallel()
	// 24 GB pool, all free, zero actual hugetlb, zero rate → tMax.
	total := uint64(24 * 1024 * 1024 * 1024)
	assert.InDelta(t, tMax, ComputeTarget(total, total, 0, 0), 0.001)
}

func TestComputeTarget_VerySlowGrowthHitsTMax(t *testing.T) {
	t.Parallel()
	total := uint64(24 * 1024 * 1024 * 1024)
	free := total
	// 1 byte/s → TTE = total seconds → way above 30 min → tMax.
	got := ComputeTarget(total, free, 0, 1)
	assert.InDelta(t, tMax, got, 0.001)
}

func TestComputeTarget_FastBucketProducesIntermediate(t *testing.T) {
	t.Parallel()
	total := uint64(24 * 1024 * 1024 * 1024)
	free := uint64(8 * 1024 * 1024 * 1024) // 33% free
	// 50 MB/s → TTE ≈ 8GB / 50MB/s ≈ 160s → "fast" bucket (>3min branch
	// is 180s; 160s falls into default which is tMin). Try 100MB/s:
	// pick a rate that lands the TTE at 5 minutes (between 3 and 10 min).
	rate := float64(free) / (5 * 60) // bytes/sec for TTE = 5min
	got := ComputeTarget(total, free, 0, rate)
	// Formula: 1 - rate * 60 / total. rate*60 = free / 5 = ~1.6 GB; / 24 GB = ~0.067; T ≈ 0.933.
	assert.InDelta(t, 0.9333, got, 0.001)
}

func TestComputeTarget_ExtremeFastHitsFloor(t *testing.T) {
	t.Parallel()
	total := uint64(24 * 1024 * 1024 * 1024)
	free := uint64(1 * 1024 * 1024 * 1024) // 4% free
	rate := 500.0 * 1024 * 1024            // 500 MB/s
	// TTE = 1GB / 500MB/s = 2s ≪ 3min → default branch → tMin.
	got := ComputeTarget(total, free, 0, rate)
	assert.InDelta(t, tMin, got, 0.001)
}

func TestComputeTarget_CushionFloorsToTMin(t *testing.T) {
	t.Parallel()
	total := uint64(24 * 1024 * 1024 * 1024)
	free := total
	// Actual usage exceeds total (degenerate but tests the floor).
	actualHugetlb := uint64(60 * 1024 * 1024 * 1024)
	got := ComputeTarget(total, free, actualHugetlb, 1)
	// cushionBased = 1 - 60*0.30/24 = -0.5 → clamped to tMin.
	assert.InDelta(t, tMin, got, 0.001)
}

func TestComputeTarget_CushionEatsTMax(t *testing.T) {
	t.Parallel()
	total := uint64(24 * 1024 * 1024 * 1024)
	free := total
	actualHugetlb := uint64(20 * 1024 * 1024 * 1024)
	got := ComputeTarget(total, free, actualHugetlb, 1)
	// cushionBased = 1 - 20*0.3/24 = 0.75; lower than tMax → wins.
	assert.InDelta(t, 0.75, got, 0.001)
}

func TestWatermarkController_LowersImmediately(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	// Default is tMax.
	got := c.Tick(0.7)
	assert.InDelta(t, 0.7, got, 0.001)
}

func TestWatermarkController_RaisesOnlyAfterHysteresis(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	// Bring it down first.
	_ = c.Tick(0.6)
	assert.InDelta(t, 0.6, c.Current(), 0.001)

	// First two raises do not move yet.
	_ = c.Tick(0.85)
	assert.InDelta(t, 0.6, c.Current(), 0.001)
	_ = c.Tick(0.85)
	assert.InDelta(t, 0.6, c.Current(), 0.001)

	// Third tick raises.
	got := c.Tick(0.85)
	assert.InDelta(t, 0.85, got, 0.001)
}

func TestWatermarkController_RaiseCappedByFirstObservation(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	_ = c.Tick(0.6)
	// First observation in rising sequence is 0.7; later ones are 0.9.
	// After confirmation we should land at 0.7, not 0.9.
	_ = c.Tick(0.7)
	_ = c.Tick(0.9)
	got := c.Tick(0.9)
	assert.InDelta(t, 0.7, got, 0.001)
}

func TestWatermarkController_LowerInMiddleResetsHysteresis(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	_ = c.Tick(0.6)
	_ = c.Tick(0.8)
	_ = c.Tick(0.8)

	// Burst pushes target down — lower immediately, hysteresis resets.
	_ = c.Tick(0.55)
	assert.InDelta(t, 0.55, c.Current(), 0.001)

	// Subsequent rise must wait the full hysteresis again.
	_ = c.Tick(0.8)
	_ = c.Tick(0.8)
	assert.InDelta(t, 0.55, c.Current(), 0.001)
	_ = c.Tick(0.8)
	assert.InDelta(t, 0.8, c.Current(), 0.001)
}

func TestWatermarkController_OOMFeedbackCapsTMax(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }

	// Baseline.
	c.RecordOOMCount(0)
	assert.InDelta(t, tMax, c.EffectiveTMax(), 0.001)

	// Two deaths in window → penalty 2 × 0.05 = 0.10.
	c.RecordOOMCount(2)
	assert.InDelta(t, tMax-0.10, c.EffectiveTMax(), 0.001)

	// Cap at oomPenaltyMax.
	c.RecordOOMCount(100)
	assert.InDelta(t, tMax-oomPenaltyMax, c.EffectiveTMax(), 0.001)
}

func TestWatermarkController_OOMFeedbackPullsTargetDown(t *testing.T) {
	t.Parallel()
	c := NewWatermarkController()
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }

	c.RecordOOMCount(0)
	c.RecordOOMCount(2) // penalty 0.10 → tMax_eff = tMax - 0.10

	// Even if target says tMax, the cap clamps us.
	got := c.Tick(tMax)
	assert.LessOrEqual(t, got, tMax-0.10+0.001)
}

func TestClamp(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0.5, clamp(0.4, 0.5, 0.9))
	assert.Equal(t, 0.9, clamp(1.1, 0.5, 0.9))
	assert.Equal(t, 0.7, clamp(0.7, 0.5, 0.9))
}
