package pressure

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRateEstimator_Empty(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	assert.Equal(t, 0.0, e.Predict())
}

func TestRateEstimator_SingleSample_AllSignalsAgree(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	e.Update(100)
	// EWMA seeded to 100, peak = 100, P95 = 100. Result = max(100, 100, 70) = 100.
	assert.Equal(t, 100.0, e.Predict())
}

func TestRateEstimator_RejectsNonFinite(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	e.Update(math.NaN())
	e.Update(math.Inf(1))
	e.Update(-50)
	assert.Equal(t, 0.0, e.Predict(), "non-finite/negative samples must be clamped to 0")
}

func TestRateEstimator_PeakDominatesAfterBurst(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	// Steady 10, then a single spike of 1000.
	for i := 0; i < 30; i++ {
		e.Update(10)
	}
	e.Update(1000)
	r := e.Predict()
	// Peak × 0.7 = 700; that should dominate the EWMA.
	assert.GreaterOrEqual(t, r, 700.0, "peak signal must lift prediction after a single spike")
}

func TestRateEstimator_P95LiftsOverEWMA(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	// 57 samples of 10, then 3 samples of 100. n = 60.
	// P95 index = ceil(0.95 * 60) - 1 = 56, sorted ascending → at idx 57 we
	// hit one of the 100s. EWMA after this sequence is ~74 (alpha=0.3).
	for i := 0; i < 57; i++ {
		e.Update(10)
	}
	for i := 0; i < 3; i++ {
		e.Update(100)
	}
	r := e.Predict()
	assert.GreaterOrEqual(t, r, 70.0)
}

func TestRateEstimator_RingBufferOverwrite(t *testing.T) {
	t.Parallel()
	e := NewRateEstimatorWith(4, 0.5, 0.7)
	// Window holds 4 samples. Push five.
	e.Update(1)
	e.Update(2)
	e.Update(3)
	e.Update(4)
	e.Update(5) // overwrites the slot that held "1"
	// Buffer is now {2,3,4,5} in some order. Peak = 5, P95 = 5 (only 4 elements).
	r := e.Predict()
	// EWMA after the sequence is between 4 and 5; peak×0.7 = 3.5; P95 = 5 → r̂ = 5.
	assert.InDelta(t, 5.0, r, 0.01)
}

func TestRateEstimator_ZeroSamples_StaysZero(t *testing.T) {
	t.Parallel()
	e := NewRateEstimator()
	for i := 0; i < 100; i++ {
		e.Update(0)
	}
	assert.Equal(t, 0.0, e.Predict())
}
