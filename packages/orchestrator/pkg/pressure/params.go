package pressure

import "time"

// Tunables for the hugepage pressure subsystem.
// See docs/design-memory-pressure-scheduling.md §8 for derivation.
const (
	tMax = 0.95
	tMin = 0.50

	ewmaAlpha        = 0.3
	rateWindowSize   = 60
	ratePeakFraction = 0.7

	tteVerySlow = 30 * time.Minute
	tteSlow     = 10 * time.Minute
	tteFast     = 3 * time.Minute
	tteSlowT    = 0.83
	targetTTE_s = 60.0

	// cushion = sum(actual Private) × factor — declared RAM overcounts.
	actualBurstFactor = 0.30

	sampleIdle  = 10 * time.Second
	sampleWarm  = 2 * time.Second
	sampleHot   = 500 * time.Millisecond
	samplePanic = 100 * time.Millisecond

	sampleWarmFrac  = 0.70
	sampleHotFrac   = 0.85
	samplePanicFrac = 0.95

	watermarkRecomputeInterval = 5 * time.Second
	watermarkUpHysteresisCount = 3

	evictTickIdle    = 1 * time.Second
	evictTickHotzone = 200 * time.Millisecond
	evictTickPanic   = 50 * time.Millisecond
	evictHotzoneFrac = 0.90
	evictPanicFrac   = 0.95
	evictTriggerFrac = 0.99
	evictStopFrac    = 0.95

	interKillDelay    = 500 * time.Millisecond
	// postBatchCooldown matches UFFD stallOnEnomem so pages freed by the round
	// reach stalled handlers within their retry window — keeps blast radius = 1.
	postBatchCooldown = 5 * time.Second
	maxKillsPerRound  = 20

	// stagnantWindow rides out async kernel reclaim (multi-GB FC unmaps can
	// take 100ms-1s on busy hosts). Bail only if free hasn't risen at all.
	stagnantWindow = 2 * time.Second

	// stopFnTimeout bounds one SIGKILL invocation. Must leave room inside
	// evictBatchBudget for reclaim + UFFD retry.
	stopFnTimeout = 2 * time.Second

	// evictBatchBudget caps a rescue round to fit the UFFD 5s stall budget
	// minus ~1s for kernel reclaim + UFFDIO_COPY retry.
	evictBatchBudget = 4 * time.Second

	// refreshTopNTimeout bounds /proc/<pid>/smaps_rollup scans — a hung read
	// must not wedge eviction; we fall back to partial results.
	refreshTopNTimeout = 500 * time.Millisecond

	oomFeedbackWindow = 10 * time.Minute
	oomPenaltyPerKill = 0.05
	oomPenaltyMax     = 0.30
)
