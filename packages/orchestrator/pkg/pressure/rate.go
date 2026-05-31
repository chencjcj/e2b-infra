package pressure

import (
	"math"
	"sort"
	"sync"
)

// RateEstimator predicts r̂ = max(EWMA, P95, peak × ratePeakFraction) bytes/sec.
type RateEstimator struct {
	mu       sync.RWMutex
	samples  []float64
	head     int
	filled   int
	ewma     float64
	hasEWMA  bool
	alpha    float64
	peakFrac float64
}

func NewRateEstimator() *RateEstimator {
	return NewRateEstimatorWith(rateWindowSize, ewmaAlpha, ratePeakFraction)
}

func NewRateEstimatorWith(windowSize int, alpha, peakFrac float64) *RateEstimator {
	if windowSize <= 0 {
		windowSize = rateWindowSize
	}
	if alpha <= 0 || alpha > 1 {
		alpha = ewmaAlpha
	}
	if peakFrac <= 0 || peakFrac > 1 {
		peakFrac = ratePeakFraction
	}
	return &RateEstimator{
		samples:  make([]float64, windowSize),
		alpha:    alpha,
		peakFrac: peakFrac,
	}
}

// Update records a bytes/sec rate sample. Non-finite or negative values clamp
// to 0 — page-release bursts must not pull the predictor below true growth.
func (e *RateEstimator) Update(instant float64) {
	if math.IsNaN(instant) || math.IsInf(instant, 0) || instant < 0 {
		instant = 0
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.samples[e.head] = instant
	e.head = (e.head + 1) % len(e.samples)
	if e.filled < len(e.samples) {
		e.filled++
	}

	if !e.hasEWMA {
		e.ewma = instant
		e.hasEWMA = true
		return
	}
	e.ewma = e.alpha*instant + (1-e.alpha)*e.ewma
}

// Predict returns r̂ in bytes/second, or 0 before any sample has been recorded.
func (e *RateEstimator) Predict() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.filled == 0 {
		return 0
	}

	// Only copy populated slots so P95/peak aren't skewed by zero-init holes.
	buf := make([]float64, e.filled)
	copy(buf, e.samples[:e.filled])
	sort.Float64s(buf)

	idx := int(math.Ceil(0.95*float64(e.filled))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= e.filled {
		idx = e.filled - 1
	}
	p95 := buf[idx]
	peak := buf[e.filled-1]

	r := e.ewma
	if p95 > r {
		r = p95
	}
	if peakScaled := peak * e.peakFrac; peakScaled > r {
		r = peakScaled
	}
	return r
}

// EWMA is exposed for tests / observability.
func (e *RateEstimator) EWMA() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ewma
}
