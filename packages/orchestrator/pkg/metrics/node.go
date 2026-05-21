package metrics

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	meminfoPath      = "/proc/meminfo"
	psiMemoryPath    = "/proc/pressure/memory"
	nodeExportPeriod = 15 * time.Second
)

// NodeObserver publishes node-level hugepage and memory-pressure metrics via
// OTEL observable gauges. The observer is independent of HostMetrics, which
// serves a different purpose (non-blocking cache for gRPC ServiceInfo).
type NodeObserver struct {
	meterExporter sdkmetric.Exporter
	registration  metric.Registration
	meter         metric.Meter

	hugepagesTotal    metric.Int64ObservableGauge
	hugepagesFree     metric.Int64ObservableGauge
	hugepagesReserved metric.Int64ObservableGauge
	hugepagesSurplus  metric.Int64ObservableGauge
	psiSomeAvg10      metric.Float64ObservableGauge
	psiFullAvg10      metric.Float64ObservableGauge

	// psiAvailable is determined once at startup: older kernels or kernels
	// without CONFIG_PSI do not expose /proc/pressure/memory.
	psiAvailable bool
}

// NewNodeObserver creates a node-level OTEL observer that periodically reports
// hugepage pool state and PSI memory pressure.
func NewNodeObserver(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string) (*NodeObserver, error) {
	meterExporter, err := telemetry.NewMeterExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create node meter exporter: %w", err)
	}

	res, err := telemetry.GetResource(ctx, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource for node observer: %w", err)
	}

	meterProvider, err := telemetry.NewMeterProvider(meterExporter, nodeExportPeriod, res, sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter))
	if err != nil {
		return nil, fmt.Errorf("failed to create node meter provider: %w", err)
	}

	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics/node")

	hpTotal, err := telemetry.GetGaugeInt(meter, telemetry.NodeHugepagesTotalBytesGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create hugepages total gauge: %w", err)
	}
	hpFree, err := telemetry.GetGaugeInt(meter, telemetry.NodeHugepagesFreeBytesGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create hugepages free gauge: %w", err)
	}
	hpReserved, err := telemetry.GetGaugeInt(meter, telemetry.NodeHugepagesReservedBytesGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create hugepages reserved gauge: %w", err)
	}
	hpSurplus, err := telemetry.GetGaugeInt(meter, telemetry.NodeHugepagesSurplusBytesGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create hugepages surplus gauge: %w", err)
	}
	psiSome, err := telemetry.GetGaugeFloat(meter, telemetry.NodeMemoryPressureSomeAvg10GaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create PSI some gauge: %w", err)
	}
	psiFull, err := telemetry.GetGaugeFloat(meter, telemetry.NodeMemoryPressureFullAvg10GaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create PSI full gauge: %w", err)
	}

	psiAvailable := true
	if _, statErr := os.Stat(psiMemoryPath); statErr != nil {
		psiAvailable = false
		logger.L().Info(ctx, "PSI memory pressure not available, skipping those metrics", zap.String("path", psiMemoryPath), zap.Error(statErr))
	}

	no := &NodeObserver{
		meterExporter:     meterExporter,
		meter:             meter,
		hugepagesTotal:    hpTotal,
		hugepagesFree:     hpFree,
		hugepagesReserved: hpReserved,
		hugepagesSurplus:  hpSurplus,
		psiSomeAvg10:      psiSome,
		psiFullAvg10:      psiFull,
		psiAvailable:      psiAvailable,
	}

	registration, err := no.startObserving()
	if err != nil {
		return nil, fmt.Errorf("failed to register node observer callback: %w", err)
	}
	no.registration = registration

	return no, nil
}

func (no *NodeObserver) startObserving() (metric.Registration, error) {
	instruments := []metric.Observable{
		no.hugepagesTotal,
		no.hugepagesFree,
		no.hugepagesReserved,
		no.hugepagesSurplus,
	}
	if no.psiAvailable {
		instruments = append(instruments, no.psiSomeAvg10, no.psiFullAvg10)
	}

	return no.meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			total, free, rsvd, surp, err := readHugepagesFromMeminfo()
			if err != nil {
				logger.L().Warn(ctx, "failed to read hugepage stats from meminfo", zap.Error(err))
			} else {
				o.ObserveInt64(no.hugepagesTotal, safeInt64(total))
				o.ObserveInt64(no.hugepagesFree, safeInt64(free))
				o.ObserveInt64(no.hugepagesReserved, safeInt64(rsvd))
				o.ObserveInt64(no.hugepagesSurplus, safeInt64(surp))
			}

			if no.psiAvailable {
				some, full, perr := readMemoryPSI()
				if perr != nil {
					logger.L().Warn(ctx, "failed to read memory PSI", zap.Error(perr))
				} else {
					o.ObserveFloat64(no.psiSomeAvg10, some)
					o.ObserveFloat64(no.psiFullAvg10, full)
				}
			}

			return nil
		}, instruments...)
}

// Close unregisters the observer callback and shuts down the exporter.
func (no *NodeObserver) Close(ctx context.Context) error {
	if no == nil || no.meterExporter == nil {
		return nil
	}

	var errs []error
	if no.registration != nil {
		if err := no.registration.Unregister(); err != nil {
			errs = append(errs, fmt.Errorf("failed to unregister node observer callback: %w", err))
		}
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, meterExporterShutdownTimeout)
	defer cancel()
	if err := no.meterExporter.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("failed to shutdown node observer meter exporter: %w", err))
	}

	return errors.Join(errs...)
}

// readHugepagesFromMeminfo returns hugepage pool totals in bytes for the
// default hugepage size configured on the node. Reads /proc/meminfo once per
// call (cheap: a few KB).
func readHugepagesFromMeminfo() (totalBytes, freeBytes, reservedBytes, surplusBytes uint64, err error) {
	f, err := os.Open(meminfoPath)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("open %s: %w", meminfoPath, err)
	}
	defer f.Close()

	var total, free, rsvd, surp, pageSizeKB uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		key := line[:colon]
		rest := line[colon+1:]

		switch key {
		case "HugePages_Total":
			total, err = parseMeminfoCount(rest)
		case "HugePages_Free":
			free, err = parseMeminfoCount(rest)
		case "HugePages_Rsvd":
			rsvd, err = parseMeminfoCount(rest)
		case "HugePages_Surp":
			surp, err = parseMeminfoCount(rest)
		case "Hugepagesize":
			pageSizeKB, err = parseMeminfoCount(rest)
		default:
			continue
		}
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("parse %s: %w", key, err)
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("scan %s: %w", meminfoPath, err)
	}

	pageSizeBytes := pageSizeKB * 1024
	return total * pageSizeBytes, free * pageSizeBytes, rsvd * pageSizeBytes, surp * pageSizeBytes, nil
}

func parseMeminfoCount(rest string) (uint64, error) {
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return 0, errors.New("missing value")
	}
	return strconv.ParseUint(fields[0], 10, 64)
}

// readMemoryPSI parses /proc/pressure/memory and returns the "some" and
// "full" avg10 percentages. Format:
//
//	some avg10=0.00 avg60=0.00 avg300=0.00 total=0
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
func readMemoryPSI() (someAvg10, fullAvg10 float64, err error) {
	f, err := os.Open(psiMemoryPath)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", psiMemoryPath, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		avg10, perr := extractAvg10(fields[1:])
		if perr != nil {
			return 0, 0, fmt.Errorf("parse %s line %q: %w", psiMemoryPath, line, perr)
		}
		switch fields[0] {
		case "some":
			someAvg10 = avg10
		case "full":
			fullAvg10 = avg10
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, fmt.Errorf("scan %s: %w", psiMemoryPath, err)
	}

	return someAvg10, fullAvg10, nil
}

func extractAvg10(pairs []string) (float64, error) {
	for _, kv := range pairs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if k == "avg10" {
			return strconv.ParseFloat(v, 64)
		}
	}
	return 0, errors.New("avg10 not found")
}

// safeInt64 clamps a uint64 to int64's positive range to satisfy the
// OTEL observable-gauge API without risk of wraparound on unrealistic values.
func safeInt64(v uint64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if v > uint64(maxInt64) {
		return maxInt64
	}
	return int64(v)
}
