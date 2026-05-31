package pressure

import (
	"sync"
	"time"
)

// WatermarkController smooths ComputeTarget into the published T with
// asymmetric hysteresis (lower fast, raise slow) and an OOM-feedback cap on tMax.
type WatermarkController struct {
	mu                      sync.Mutex
	current                 float64
	upCounter               int
	targetWhenStartedRising float64

	tMax              float64
	tMin              float64
	upHysteresisCount int

	now func() time.Time

	// Cumulative OOM-counter snapshots; deaths-in-window = latest - oldest.
	oomSamples []oomSample
}

type oomSample struct {
	ts    time.Time
	count uint64
}

// NewWatermarkController starts with T = tMax (admission fully open).
func NewWatermarkController() *WatermarkController {
	return &WatermarkController{
		current:           tMax,
		tMax:              tMax,
		tMin:              tMin,
		upHysteresisCount: watermarkUpHysteresisCount,
		now:               time.Now,
	}
}

// ComputeTarget is the pure pre-hysteresis target. total/free/actualHugetlb
// are bytes, ratePredict is bytes/sec. Returns 1.0 when the node has no pool.
func ComputeTarget(total, free, actualHugetlb uint64, ratePredict float64) float64 {
	if total == 0 {
		return 1.0
	}

	// Floor the rate so steady-state TTE doesn't blow up to +Inf.
	rate := ratePredict
	if rate < 1.0 {
		rate = 1.0
	}
	tteSec := float64(free) / rate

	var t float64
	switch {
	case tteSec > tteVerySlow.Seconds():
		t = tMax
	case tteSec > tteSlow.Seconds():
		t = tteSlowT
	case tteSec > tteFast.Seconds():
		// Interpolate so the watermark equals the projected free fraction
		// targetTTE_s seconds out.
		t = 1.0 - ratePredict*targetTTE_s/float64(total)
	default:
		t = tMin
	}

	cushionBased := 1.0 - float64(actualHugetlb)*actualBurstFactor/float64(total)
	if cushionBased < t {
		t = cushionBased
	}
	return clamp(t, tMin, tMax)
}

// RecordOOMCount records the latest cumulative OOM counter. One pre-cutoff
// sample is retained as the baseline for "deaths since cutoff".
func (c *WatermarkController) RecordOOMCount(count uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.oomSamples = append(c.oomSamples, oomSample{ts: now, count: count})

	cutoff := now.Add(-oomFeedbackWindow)
	idx := 0
	for ; idx < len(c.oomSamples); idx++ {
		if !c.oomSamples[idx].ts.Before(cutoff) {
			break
		}
	}
	if idx > 1 {
		c.oomSamples = append(c.oomSamples[:0], c.oomSamples[idx-1:]...)
	}
}

func (c *WatermarkController) effectiveTMax() float64 {
	if len(c.oomSamples) == 0 {
		return c.tMax
	}
	latest := c.oomSamples[len(c.oomSamples)-1].count
	baseline := c.oomSamples[0].count
	if latest <= baseline {
		return c.tMax
	}
	deaths := latest - baseline
	penalty := float64(deaths) * oomPenaltyPerKill
	if penalty > oomPenaltyMax {
		penalty = oomPenaltyMax
	}
	t := c.tMax - penalty
	if t < c.tMin {
		t = c.tMin
	}
	return t
}

// Tick advances the controller toward target and returns the new T.
func (c *WatermarkController) Tick(target float64) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	tMaxEff := c.effectiveTMax()
	if target > tMaxEff {
		target = tMaxEff
	}
	if target < c.tMin {
		target = c.tMin
	}
	// Snap down on the same tick a fresh OOM is observed.
	if c.current > tMaxEff {
		c.current = tMaxEff
		c.upCounter = 0
	}

	switch {
	case target < c.current:
		c.current = target
		c.upCounter = 0
	case target > c.current:
		if c.upCounter == 0 {
			c.targetWhenStartedRising = target
		}
		c.upCounter++
		if c.upCounter >= c.upHysteresisCount {
			cap := target
			if c.targetWhenStartedRising < cap {
				cap = c.targetWhenStartedRising
			}
			c.current = cap
			c.upCounter = 0
		}
	default:
		c.upCounter = 0
	}
	return c.current
}

func (c *WatermarkController) Current() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *WatermarkController) EffectiveTMax() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.effectiveTMax()
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
