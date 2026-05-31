package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type (
	CounterType                 string
	ObservableCounterType       string
	GaugeFloatType              string
	GaugeIntType                string
	UpDownCounterType           string
	ObservableUpDownCounterType string
	HistogramType               string
)

const (
	ApiOrchestratorCreatedSandboxes CounterType = "api.orchestrator.created_sandboxes"
	SandboxCreateMeterName          CounterType = "api.env.instance.started"

	TeamSandboxCreated CounterType = "e2b.team.sandbox.created"

	EnvdInitCalls CounterType = "orchestrator.sandbox.envd.init.calls"
)

const (
	ApiOrchestratorSbxCreateSuccess ObservableCounterType = "api.orchestrator.sandbox.create.success"
	ApiOrchestratorSbxCreateFailure ObservableCounterType = "api.orchestrator.sandbox.create.failure"
)

const (
	OrchestratorSandboxCountMeterName ObservableUpDownCounterType = "orchestrator.env.sandbox.running"

	ClientProxyServerConnectionsMeterCounterName ObservableUpDownCounterType = "client_proxy.proxy.server.connections.open"
	ClientProxyPoolConnectionsMeterCounterName   ObservableUpDownCounterType = "client_proxy.proxy.pool.connections.open"
	ClientProxyPoolSizeMeterCounterName          ObservableUpDownCounterType = "client_proxy.proxy.pool.size"

	OrchestratorProxyServerConnectionsMeterCounterName ObservableUpDownCounterType = "orchestrator.proxy.server.connections.open"
	OrchestratorProxyPoolConnectionsMeterCounterName   ObservableUpDownCounterType = "orchestrator.proxy.pool.connections.open"
	OrchestratorProxyPoolSizeMeterCounterName          ObservableUpDownCounterType = "orchestrator.proxy.pool.size"

	BuildCounterMeterName ObservableUpDownCounterType = "api.env.build.running"

	TCPFirewallActiveConnections ObservableUpDownCounterType = "orchestrator.tcpfirewall.connections.active"
)

const (
	SandboxCpuUsedGaugeName GaugeFloatType = "e2b.sandbox.cpu.used"

	// PSI from /proc/pressure/memory.
	NodeMemoryPressureSomeAvg10GaugeName GaugeFloatType = "e2b.node.memory.pressure.some.avg10"
	NodeMemoryPressureFullAvg10GaugeName GaugeFloatType = "e2b.node.memory.pressure.full.avg10"

	// Hugepage memory-pressure scheduling — dynamic watermark and rate estimator outputs.
	NodeHugepagesWatermarkGaugeName     GaugeFloatType = "e2b.node.hugepages.watermark"
	NodeHugepagesGrowthRateBytesGauge   GaugeFloatType = "e2b.node.hugepages.growth_rate_bytes"
	NodeHugepagesTTESecondsGaugeName    GaugeFloatType = "e2b.node.hugepages.tte_seconds"
)

const (
	// Build timing histograms
	BuildDurationHistogramName      HistogramType = "template.build.duration"
	BuildPhaseDurationHistogramName HistogramType = "template.build.phase.duration"
	BuildStepDurationHistogramName  HistogramType = "template.build.step.duration"

	// Sandbox timing histograms
	OrchestratorSandboxCreateDurationName HistogramType = "orchestrator.sandbox.create.duration"
	WaitForEnvdDurationHistogramName      HistogramType = "orchestrator.sandbox.envd.init.duration"

	// TCP Firewall histograms
	TCPFirewallConnectionDurationHistogramName    HistogramType = "orchestrator.tcpfirewall.connection.duration"
	TCPFirewallConnectionsPerSandboxHistogramName HistogramType = "orchestrator.tcpfirewall.connections.per_sandbox"

	// Ingress proxy histograms
	IngressProxyConnectionDurationHistogramName    HistogramType = "orchestrator.proxy.connection.duration"
	IngressProxyConnectionsPerSandboxHistogramName HistogramType = "orchestrator.proxy.connections.per_sandbox"
)

const (
	// Build result counters
	BuildResultCounterName      CounterType = "template.build.result"
	BuildCacheResultCounterName CounterType = "template.build.cache.result"

	// TCP Firewall counters
	TCPFirewallConnectionsTotal CounterType = "orchestrator.tcpfirewall.connections.total"
	TCPFirewallErrorsTotal      CounterType = "orchestrator.tcpfirewall.errors.total"
	TCPFirewallDecisionsTotal   CounterType = "orchestrator.tcpfirewall.decisions.total"

	// Ingress proxy counters
	IngressProxyConnectionsBlockedTotal CounterType = "orchestrator.proxy.connections.blocked.total"

	// cmux counters
	CmuxErrorsTotal CounterType = "orchestrator.cmux.errors.total"

	// Firecracker net counters — global totals, no sandbox_id (low cardinality).
	// All carry a direction=tx/rx attribute. Per-sandbox distributions are histograms below.
	SandboxFCNetFails         CounterType = "orchestrator.sandbox.fc.net.fails"
	SandboxFCNetNoAvailBuffer CounterType = "orchestrator.sandbox.fc.net.no_avail_buffer"
	SandboxFCNetTapIOFails    CounterType = "orchestrator.sandbox.fc.net.tap_io_fails"

	// Firecracker block counters — global totals, no sandbox_id (low cardinality).
	// Carry a direction=read/write attribute where applicable.
	SandboxFCBlockFails         CounterType = "orchestrator.sandbox.fc.block.fails"
	SandboxFCBlockNoAvailBuffer CounterType = "orchestrator.sandbox.fc.block.no_avail_buffer"

	// outcome=retry_succeeded|retry_exhausted|ctx_cancelled|other_error.
	// retry_exhausted is the alert signal (stall budget elapsed → sandbox dies).
	SandboxUffdCopyEnomemTotal CounterType = "orchestrator.sandbox.uffd.copy_enomem.total"
	NodeHugepageOomTotal       CounterType = "e2b.node.hugepage_oom.total"
	// outcome=success|failure
	NodePressureEvictTotal CounterType = "e2b.node.pressure.evict_total"
	// outcome=drained|stagnant|max_kills|deficit_skip|topn_empty|meminfo_error|budget_exhausted
	NodePressureEvictRoundTotal CounterType = "e2b.node.pressure.evict_round_total"
	// Cooldown bypassed at panicFrac — early warning that pool is undersized.
	NodePressureCooldownBypassTotal CounterType = "e2b.node.pressure.cooldown_bypass_total"

	// PagepoolPopulateSkippedTotal: memfd populate skipped because the
	// pressure probe (pool ≥ pagepoolPressureFrac) said it's not safe.
	// reason: "pressure" on the SIGBUS-defense path. High counts mean
	// sharing effectiveness is being throttled by host load — sandboxes
	// fall back to private (UFFDIO_COPY) for these pages.
	PagepoolPopulateSkippedTotal CounterType = "e2b.pagepool.populate_skipped_total"
)

const (
	// Firecracker net histograms — per-sandbox distribution per metrics flush, no sandbox_id.
	// Firecracker serializes SharedIncMetric as per-flush deltas (default flush interval: 60 s).
	// Symmetric TX/RX metrics carry a direction=tx/rx attribute; TX-only metrics always use direction=tx.
	SandboxFCNetBytes                HistogramType = "orchestrator.sandbox.fc.net.bytes"
	SandboxFCNetPackets              HistogramType = "orchestrator.sandbox.fc.net.packets"
	SandboxFCNetCount                HistogramType = "orchestrator.sandbox.fc.net.count"
	SandboxFCNetRateLimiterThrottled HistogramType = "orchestrator.sandbox.fc.net.rate_limiter_throttled"
	// TX-only: no RX equivalent in Firecracker metrics.
	SandboxFCNetRateLimiterEventCount HistogramType = "orchestrator.sandbox.fc.net.rate_limiter_event_count"
	SandboxFCNetRemainingReqs         HistogramType = "orchestrator.sandbox.fc.net.remaining_reqs"

	// outcome=retry_succeeded|retry_exhausted|ctx_cancelled|other_error
	SandboxUffdStallDuration HistogramType = "orchestrator.sandbox.uffd.stall_duration"

	// outcome=success|failure
	NodePressureEvictDurationMs HistogramType = "e2b.node.pressure.evict_duration_ms"

	// Firecracker block histograms — per-sandbox distribution per metrics flush, no sandbox_id.
	// Symmetric read/write metrics carry a direction=read/write attribute.
	SandboxFCBlockBytes                 HistogramType = "orchestrator.sandbox.fc.block.bytes"
	SandboxFCBlockCount                 HistogramType = "orchestrator.sandbox.fc.block.count"
	SandboxFCBlockRateLimiterThrottled  HistogramType = "orchestrator.sandbox.fc.block.rate_limiter_throttled"
	SandboxFCBlockRateLimiterEventCount HistogramType = "orchestrator.sandbox.fc.block.rate_limiter_event_count"
	SandboxFCBlockIOEngineThrottled     HistogramType = "orchestrator.sandbox.fc.block.io_engine_throttled"
	SandboxFCBlockRemainingReqs         HistogramType = "orchestrator.sandbox.fc.block.remaining_reqs"
)

const (
	ApiOrchestratorCountMeterName GaugeIntType = "api.orchestrator.status"

	// Sandbox metrics
	SandboxRamUsedGaugeName   GaugeIntType = "e2b.sandbox.ram.used"
	SandboxRamTotalGaugeName  GaugeIntType = "e2b.sandbox.ram.total"
	SandboxRamCacheGaugeName  GaugeIntType = "e2b.sandbox.ram.cache"
	SandboxCpuTotalGaugeName  GaugeIntType = "e2b.sandbox.cpu.total"
	SandboxDiskUsedGaugeName  GaugeIntType = "e2b.sandbox.disk.used"
	SandboxDiskTotalGaugeName GaugeIntType = "e2b.sandbox.disk.total"

	// Sandbox host-side metrics (sourced from /proc and cgroup, not envd).
	SandboxHugepagesUsedGaugeName GaugeIntType = "e2b.sandbox.hugepages.used_bytes"
	SandboxMemoryCurrentGaugeName GaugeIntType = "e2b.sandbox.memory.current_bytes"

	// Node hugepage pool from /proc/meminfo.
	NodeHugepagesTotalBytesGaugeName    GaugeIntType = "e2b.node.hugepages.total_bytes"
	NodeHugepagesFreeBytesGaugeName     GaugeIntType = "e2b.node.hugepages.free_bytes"
	NodeHugepagesReservedBytesGaugeName GaugeIntType = "e2b.node.hugepages.reserved_bytes"
	NodeHugepagesSurplusBytesGaugeName  GaugeIntType = "e2b.node.hugepages.surplus_bytes"

	// Team metrics
	TeamSandboxRunningGaugeName GaugeIntType = "e2b.team.sandbox.running"

	SandboxCountGaugeName GaugeIntType = "api.env.instance.running"

	// Build resource metrics
	BuildRootfsSizeHistogramName HistogramType = "template.build.rootfs.size"
)

var counterDesc = map[CounterType]string{
	SandboxCreateMeterName:          "Number of currently waiting requests to create a new sandbox",
	ApiOrchestratorCreatedSandboxes: "Number of successfully created sandboxes",
	BuildResultCounterName:          "Number of template build results",
	BuildCacheResultCounterName:     "Number of build cache results",
	TeamSandboxCreated:              "Counter of started sandboxes for the team in the interval",
	EnvdInitCalls:                   "Number of envd initialization calls",
	TCPFirewallConnectionsTotal:     "Total number of TCP firewall connections processed",
	TCPFirewallErrorsTotal:          "Total number of TCP firewall errors",
	TCPFirewallDecisionsTotal:       "Total number of TCP firewall allow/block decisions",

	IngressProxyConnectionsBlockedTotal: "Total number of ingress proxy connections blocked by connection limit",
	CmuxErrorsTotal:                     "Total number of cmux connection multiplexer errors",

	SandboxFCNetFails:         "Total Firecracker VMM errors transmitting or receiving data (direction=tx/rx)",
	SandboxFCNetNoAvailBuffer: "Total Firecracker VMM events where no virtqueue buffer was available (direction=tx/rx)",
	SandboxFCNetTapIOFails:    "Total Firecracker VMM TAP I/O failures (direction=tx/rx)",

	SandboxFCBlockFails:         "Total Firecracker VMM block device execution/event failures",
	SandboxFCBlockNoAvailBuffer: "Total Firecracker VMM block events where no virtqueue buffer was available",

	SandboxUffdCopyEnomemTotal: "UFFDIO_COPY calls that hit ENOMEM (hugepage pool exhausted), bucketed by outcome of the stall retry loop.",
	NodeHugepageOomTotal:       "Sandboxes terminated because UFFDIO_COPY could not be served within the stall budget after exhausting retries.",
	NodePressureEvictTotal:          "Sandboxes evicted by the memory-pressure controller to reclaim hugepages, bucketed by outcome.",
	NodePressureEvictRoundTotal:     "Memory-pressure rescue rounds (one per evictBatch invocation), bucketed by terminal state (drained|stagnant|max_kills|deficit_skip|topn_empty|meminfo_error).",
	NodePressureCooldownBypassTotal: "Times the post-batch cooldown was bypassed because usage climbed back to panicFrac during cooldown — pool is undersized vs incoming pressure.",
	PagepoolPopulateSkippedTotal:    "Memfd populate calls skipped because the host hugepage pool was too close to full to safely write — falls back to private UFFDIO_COPY for that page.",
}

var counterUnits = map[CounterType]string{
	SandboxCreateMeterName:          "{sandbox}",
	ApiOrchestratorCreatedSandboxes: "{sandbox}",
	BuildResultCounterName:          "{build}",
	BuildCacheResultCounterName:     "{layer}",
	TeamSandboxCreated:              "{sandbox}",
	EnvdInitCalls:                   "1",
	TCPFirewallConnectionsTotal:     "{connection}",
	TCPFirewallErrorsTotal:          "{error}",
	TCPFirewallDecisionsTotal:       "{decision}",

	IngressProxyConnectionsBlockedTotal: "{connection}",
	CmuxErrorsTotal:                     "{error}",

	SandboxFCNetFails:         "{error}",
	SandboxFCNetNoAvailBuffer: "{event}",
	SandboxFCNetTapIOFails:    "{error}",

	SandboxFCBlockFails:         "{error}",
	SandboxFCBlockNoAvailBuffer: "{event}",

	SandboxUffdCopyEnomemTotal:  "{event}",
	NodeHugepageOomTotal:        "{sandbox}",
	NodePressureEvictTotal:          "{sandbox}",
	NodePressureEvictRoundTotal:     "{round}",
	NodePressureCooldownBypassTotal: "{event}",
	PagepoolPopulateSkippedTotal:    "{page}",
}

var observableCounterDesc = map[ObservableCounterType]string{
	ApiOrchestratorSbxCreateSuccess: "Counter of successful sandbox creation requests.",
	ApiOrchestratorSbxCreateFailure: "Counter of failed sandbox creation requests.",
}

var observableCounterUnits = map[ObservableCounterType]string{
	ApiOrchestratorSbxCreateSuccess: "{sandbox}",
	ApiOrchestratorSbxCreateFailure: "{sandbox}",
}

var upDownCounterDesc = map[UpDownCounterType]string{}

var upDownCounterUnits = map[UpDownCounterType]string{}

var observableUpDownCounterDesc = map[ObservableUpDownCounterType]string{
	OrchestratorSandboxCountMeterName:                  "Counter of running sandboxes on the orchestrator.",
	ClientProxyServerConnectionsMeterCounterName:       "Open connections to the client proxy from load balancer.",
	ClientProxyPoolConnectionsMeterCounterName:         "Open connections from the client proxy to the orchestrator proxy.",
	ClientProxyPoolSizeMeterCounterName:                "Size of the client proxy pool.",
	OrchestratorProxyServerConnectionsMeterCounterName: "Open connections to the orchestrator proxy from client proxies.",
	OrchestratorProxyPoolConnectionsMeterCounterName:   "Open connections from the orchestrator proxy to sandboxes.",
	OrchestratorProxyPoolSizeMeterCounterName:          "Size of the orchestrator proxy pool.",
	BuildCounterMeterName:                              "Counter of running builds.",

	TCPFirewallActiveConnections: "Number of currently active TCP firewall connections.",
}

var observableUpDownCounterUnits = map[ObservableUpDownCounterType]string{
	OrchestratorSandboxCountMeterName:                  "{sandbox}",
	ClientProxyServerConnectionsMeterCounterName:       "{connection}",
	ClientProxyPoolConnectionsMeterCounterName:         "{connection}",
	ClientProxyPoolSizeMeterCounterName:                "{transport}",
	OrchestratorProxyServerConnectionsMeterCounterName: "{connection}",
	OrchestratorProxyPoolConnectionsMeterCounterName:   "{connection}",
	OrchestratorProxyPoolSizeMeterCounterName:          "{transport}",
	BuildCounterMeterName:                              "{build}",

	TCPFirewallActiveConnections: "{connection}",
}

var gaugeFloatDesc = map[GaugeFloatType]string{
	SandboxCpuUsedGaugeName:              "Amount of CPU used by the sandbox.",
	NodeMemoryPressureSomeAvg10GaugeName: "Node-wide PSI memory pressure (some) averaged over 10 seconds.",
	NodeMemoryPressureFullAvg10GaugeName: "Node-wide PSI memory pressure (full) averaged over 10 seconds.",
	NodeHugepagesWatermarkGaugeName:      "Current dynamic hugepage admission watermark T (used/total threshold below which the node accepts new sandboxes).",
	NodeHugepagesGrowthRateBytesGauge:    "Predicted near-term hugepage growth rate r̂ = max(EWMA, P95, peak×0.7) in bytes per second.",
	NodeHugepagesTTESecondsGaugeName:     "Predicted time-to-exhaustion of the hugepage pool at the current rate (free / r̂), in seconds.",
}

var gaugeFloatUnits = map[GaugeFloatType]string{
	SandboxCpuUsedGaugeName:              "{percent}",
	NodeMemoryPressureSomeAvg10GaugeName: "{percent}",
	NodeMemoryPressureFullAvg10GaugeName: "{percent}",
	NodeHugepagesWatermarkGaugeName:      "1",
	NodeHugepagesGrowthRateBytesGauge:    "{By}/s",
	NodeHugepagesTTESecondsGaugeName:     "s",
}

var gaugeIntDesc = map[GaugeIntType]string{
	ApiOrchestratorCountMeterName:       "Counter of running orchestrators.",
	SandboxRamUsedGaugeName:             "Amount of RAM used by the sandbox.",
	SandboxRamTotalGaugeName:            "Amount of RAM available to the sandbox.",
	SandboxRamCacheGaugeName:            "Amount of RAM used by the page cache in the sandbox.",
	SandboxCpuTotalGaugeName:            "Amount of CPU available to the sandbox.",
	SandboxDiskUsedGaugeName:            "Amount of disk space used by the sandbox.",
	SandboxDiskTotalGaugeName:           "Amount of disk space available to the sandbox.",
	SandboxHugepagesUsedGaugeName:       "Hugepage bytes mapped into the sandbox's Firecracker process (Private_Hugetlb + Shared_Hugetlb from smaps_rollup).",
	SandboxMemoryCurrentGaugeName:       "Current memory usage of the sandbox cgroup (memory.current).",
	NodeHugepagesTotalBytesGaugeName:    "Total hugepage pool size on the node.",
	NodeHugepagesFreeBytesGaugeName:     "Free hugepage pool size on the node.",
	NodeHugepagesReservedBytesGaugeName: "Hugepages reserved but not yet allocated on the node.",
	NodeHugepagesSurplusBytesGaugeName:  "Surplus hugepages dynamically allocated above the configured pool.",
	TeamSandboxRunningGaugeName:         "The number of sandboxes running for the team in the interval.",
	SandboxCountGaugeName:               "Number of running sandbox instances per team.",
}

var gaugeIntUnits = map[GaugeIntType]string{
	ApiOrchestratorCountMeterName:       "{orchestrator}",
	SandboxRamUsedGaugeName:             "{By}",
	SandboxRamTotalGaugeName:            "{By}",
	SandboxRamCacheGaugeName:            "{By}",
	SandboxCpuTotalGaugeName:            "{count}",
	SandboxDiskUsedGaugeName:            "{By}",
	SandboxDiskTotalGaugeName:           "{By}",
	SandboxHugepagesUsedGaugeName:       "{By}",
	SandboxMemoryCurrentGaugeName:       "{By}",
	NodeHugepagesTotalBytesGaugeName:    "{By}",
	NodeHugepagesFreeBytesGaugeName:     "{By}",
	NodeHugepagesReservedBytesGaugeName: "{By}",
	NodeHugepagesSurplusBytesGaugeName:  "{By}",
	TeamSandboxRunningGaugeName:         "{sandbox}",
	SandboxCountGaugeName:               "{sandbox}",
}

func GetCounter(meter metric.Meter, name CounterType) (metric.Int64Counter, error) {
	desc := counterDesc[name]
	unit := counterUnits[name]

	return meter.Int64Counter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetUpDownCounter(meter metric.Meter, name UpDownCounterType) (metric.Int64UpDownCounter, error) {
	desc := upDownCounterDesc[name]
	unit := upDownCounterUnits[name]

	return meter.Int64UpDownCounter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetObservableCounter(meter metric.Meter, name ObservableCounterType, callback metric.Int64Callback) (metric.Int64ObservableCounter, error) {
	desc := observableCounterDesc[name]
	unit := observableCounterUnits[name]

	return meter.Int64ObservableCounter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
		metric.WithInt64Callback(callback),
	)
}

func GetObservableUpDownCounter(meter metric.Meter, name ObservableUpDownCounterType, callback metric.Int64Callback) (metric.Int64ObservableUpDownCounter, error) {
	desc := observableUpDownCounterDesc[name]
	unit := observableUpDownCounterUnits[name]

	return meter.Int64ObservableUpDownCounter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
		metric.WithInt64Callback(callback),
	)
}

func GetGaugeFloat(meter metric.Meter, name GaugeFloatType) (metric.Float64ObservableGauge, error) {
	desc := gaugeFloatDesc[name]
	unit := gaugeFloatUnits[name]

	return meter.Float64ObservableGauge(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetGaugeInt(meter metric.Meter, name GaugeIntType) (metric.Int64ObservableGauge, error) {
	desc := gaugeIntDesc[name]
	unit := gaugeIntUnits[name]

	return meter.Int64ObservableGauge(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

var histogramDesc = map[HistogramType]string{
	BuildDurationHistogramName:            "Time taken to build a template",
	BuildPhaseDurationHistogramName:       "Time taken to build each phase of a template",
	BuildStepDurationHistogramName:        "Time taken to build each step of a template",
	BuildRootfsSizeHistogramName:          "Size of the built template rootfs in bytes",
	OrchestratorSandboxCreateDurationName: "Time taken to create a sandbox",
	WaitForEnvdDurationHistogramName:      "Time taken for Envd to initialize successfully",

	TCPFirewallConnectionDurationHistogramName:    "Duration of TCP firewall proxied connections",
	TCPFirewallConnectionsPerSandboxHistogramName: "Number of active TCP firewall connections per sandbox",

	IngressProxyConnectionDurationHistogramName:    "Duration of ingress proxy connections",
	IngressProxyConnectionsPerSandboxHistogramName: "Number of active ingress proxy connections per sandbox",

	// Firecracker net histograms (direction=tx/rx attribute; TX-only carry direction=tx)
	SandboxFCNetBytes:                 "Distribution of Firecracker VMM bytes per metrics flush",
	SandboxFCNetPackets:               "Distribution of Firecracker VMM packets per metrics flush",
	SandboxFCNetCount:                 "Distribution of Firecracker VMM I/O operations per metrics flush",
	SandboxFCNetRateLimiterThrottled:  "Distribution of Firecracker VMM ops throttled by rate limiter per metrics flush",
	SandboxFCNetRateLimiterEventCount: "Distribution of Firecracker VMM TX rate limiter events per metrics flush",
	SandboxFCNetRemainingReqs:         "Distribution of Firecracker VMM TX queue remaining-request events per metrics flush",

	SandboxUffdStallDuration:    "Duration spent waiting for UFFDIO_COPY to succeed after hitting ENOMEM (with outcome=retry_succeeded|retry_exhausted attribute).",
	NodePressureEvictDurationMs: "Wall-clock duration from pressure-eviction trigger to SIGKILL completion (with outcome=success|failure attribute).",

	// Firecracker block histograms (direction=read/write attribute)
	SandboxFCBlockBytes:                 "Distribution of Firecracker VMM block bytes per metrics flush",
	SandboxFCBlockCount:                 "Distribution of Firecracker VMM block I/O operations per metrics flush",
	SandboxFCBlockRateLimiterThrottled:  "Distribution of Firecracker VMM block ops throttled by rate limiter per metrics flush",
	SandboxFCBlockRateLimiterEventCount: "Distribution of Firecracker VMM block rate limiter events per metrics flush",
	SandboxFCBlockIOEngineThrottled:     "Distribution of Firecracker VMM block ops throttled by io_uring engine per metrics flush",
	SandboxFCBlockRemainingReqs:         "Distribution of Firecracker VMM block queue remaining-request events per metrics flush",
}

var histogramUnits = map[HistogramType]string{
	BuildDurationHistogramName:                    "ms",
	BuildPhaseDurationHistogramName:               "ms",
	BuildStepDurationHistogramName:                "ms",
	BuildRootfsSizeHistogramName:                  "{By}",
	OrchestratorSandboxCreateDurationName:         "ms",
	WaitForEnvdDurationHistogramName:              "ms",
	TCPFirewallConnectionDurationHistogramName:    "ms",
	TCPFirewallConnectionsPerSandboxHistogramName: "{connection}",

	IngressProxyConnectionDurationHistogramName:    "ms",
	IngressProxyConnectionsPerSandboxHistogramName: "{connection}",

	// Firecracker net histograms
	SandboxFCNetBytes:                 "{By}",
	SandboxFCNetPackets:               "{packet}",
	SandboxFCNetCount:                 "{op}",
	SandboxFCNetRateLimiterThrottled:  "{op}",
	SandboxFCNetRateLimiterEventCount: "{event}",
	SandboxFCNetRemainingReqs:         "{event}",

	SandboxUffdStallDuration:    "ms",
	NodePressureEvictDurationMs: "ms",

	// Firecracker block histograms
	SandboxFCBlockBytes:                 "{By}",
	SandboxFCBlockCount:                 "{op}",
	SandboxFCBlockRateLimiterThrottled:  "{op}",
	SandboxFCBlockRateLimiterEventCount: "{event}",
	SandboxFCBlockIOEngineThrottled:     "{op}",
	SandboxFCBlockRemainingReqs:         "{event}",
}

func GetHistogram(meter metric.Meter, name HistogramType) (metric.Int64Histogram, error) {
	desc := histogramDesc[name]
	unit := histogramUnits[name]

	return meter.Int64Histogram(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

type TimerFactory struct {
	duration metric.Int64Histogram
	bytes    metric.Int64Counter
	count    metric.Int64Counter
}

func NewTimerFactory(
	blocksMeter metric.Meter,
	metricName, durationDescription, bytesDescription, counterDescription string,
) (TimerFactory, error) {
	duration, err := blocksMeter.Int64Histogram(metricName,
		metric.WithDescription(durationDescription),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to get slices metric: %w", err)
	}

	bytes, err := blocksMeter.Int64Counter(metricName,
		metric.WithDescription(bytesDescription),
		metric.WithUnit("By"),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to create total bytes requested metric: %w", err)
	}

	count, err := blocksMeter.Int64Counter(metricName,
		metric.WithDescription(counterDescription),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to create total page faults metric: %w", err)
	}

	return TimerFactory{duration, bytes, count}, nil
}

func (f *TimerFactory) Begin(kv ...attribute.KeyValue) *Stopwatch {
	return &Stopwatch{
		histogram: f.duration,
		sum:       f.bytes,
		count:     f.count,
		start:     time.Now(),
		kv:        kv,
	}
}

type Stopwatch struct {
	histogram  metric.Int64Histogram
	sum, count metric.Int64Counter
	start      time.Time
	kv         []attribute.KeyValue
}

const (
	resultAttr        = "result"
	resultTypeSuccess = "success"
	resultTypeFailure = "failure"
)

var (
	// Pre-allocated result attributes for use with PrecomputeAttrs.
	Success = attribute.String(resultAttr, resultTypeSuccess)
	Failure = attribute.String(resultAttr, resultTypeFailure)
)

func (t Stopwatch) Success(ctx context.Context, total int64, kv ...attribute.KeyValue) {
	t.end(ctx, resultTypeSuccess, total, kv...)
}

func (t Stopwatch) Failure(ctx context.Context, total int64, kv ...attribute.KeyValue) {
	t.end(ctx, resultTypeFailure, total, kv...)
}

func (t Stopwatch) end(ctx context.Context, result string, total int64, kv ...attribute.KeyValue) {
	kv = append(kv, attribute.KeyValue{Key: resultAttr, Value: attribute.StringValue(result)})
	kv = append(t.kv, kv...)
	opt := metric.WithAttributeSet(attribute.NewSet(kv...))
	t.RecordRaw(ctx, total, opt)
}

// PrecomputeAttrs builds a reusable MeasurementOption from the given attribute
// key-values. The option must include all attributes (including "result").
// Use with Stopwatch.Record to avoid per-call attribute allocation.
func PrecomputeAttrs(kv ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(kv...))
}

// RecordRaw records an operation using a precomputed attribute option, it does
// not include any previous attributes passed at Begin(). Zero-allocation
// alternative to Success/Failure for hot paths.
func (t Stopwatch) RecordRaw(ctx context.Context, total int64, precomputedAttrs metric.MeasurementOption) {
	amount := time.Since(t.start).Milliseconds()
	t.histogram.Record(ctx, amount, precomputedAttrs)
	t.sum.Add(ctx, total, precomputedAttrs)
	t.count.Add(ctx, 1, precomputedAttrs)
}
