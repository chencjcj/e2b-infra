package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/apiclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/rdma"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Matches the template cache TTL so entries live as long as the
// templates they refer to and are cleaned up automatically.
const uploadedBuildsTTL = 1 * time.Hour

type Server struct {
	orchestrator.UnimplementedSandboxServiceServer
	orchestrator.UnimplementedChunkServiceServer

	config                cfg.Config
	sandboxFactory        *sandbox.Factory
	info                  *service.ServiceInfo
	proxy                 *proxy.SandboxProxy
	networkPool           *network.Pool
	templateCache         *template.Cache
	devicePool            *nbd.DevicePool
	persistence           storage.StorageProvider
	featureFlags          *featureflags.Client
	sbxEventsService      *events.EventsService
	startingSandboxes     *semaphore.Weighted
	pausingSandboxes      *utils.AdjustableSemaphore
	peerRegistry          peerclient.Registry
	uploadedBuilds        *ttlcache.Cache[string, struct{}]
	sandboxCreateDuration metric.Int64Histogram

	rdmaConfig rdma.Config
	apiClient  *apiclient.Client

	// migrations: in-flight RDMA migration sessions keyed by sandboxID.
	migrationsMu sync.Mutex
	migrations   map[string]*migrationSession
}

type ServiceConfig struct {
	Config           cfg.Config
	Tel              *telemetry.Client
	NetworkPool      *network.Pool
	DevicePool       *nbd.DevicePool
	TemplateCache    *template.Cache
	Info             *service.ServiceInfo
	Proxy            *proxy.SandboxProxy
	SandboxFactory   *sandbox.Factory
	Persistence      storage.StorageProvider
	FeatureFlags     *featureflags.Client
	SbxEventsService *events.EventsService
	PeerRegistry     peerclient.Registry
}

func New(cfg ServiceConfig) (*Server, error) {
	uploadedBuilds := ttlcache.New[string, struct{}](
		ttlcache.WithTTL[string, struct{}](uploadedBuildsTTL),
	)
	go uploadedBuilds.Start()

	// Per-node auto-resolution lets one shared deployment template work
	// across nodes with different RDMA NIC names + IPs. RDMA_SUBNET is the
	// strongest hint; falls back to RDMA_DEVICE or first eligible device.
	rdmaDevice := cfg.Config.RDMADevice
	advertiseAddr := cfg.Config.RDMAAdvertiseAddr
	if cfg.Config.RDMASourceBinary != "" || cfg.Config.RDMADestBinary != "" {
		if advertiseAddr == "" || rdmaDevice == "" {
			resolvedDev, resolvedAddr, err := rdma.ResolveDeviceAndAddr(rdmaDevice, cfg.Config.RDMASubnet)
			if err != nil {
				logger.L().Warn(context.Background(),
					"RDMA device/addr auto-resolution failed; migration will rely on API-side fallback or fail",
					zap.String("hint_device", rdmaDevice),
					zap.String("hint_subnet", cfg.Config.RDMASubnet),
					zap.Error(err),
				)
			} else {
				if rdmaDevice == "" {
					rdmaDevice = resolvedDev
				}
				if advertiseAddr == "" {
					advertiseAddr = resolvedAddr
				}
				logger.L().Info(context.Background(),
					"RDMA device + advertise address auto-resolved",
					zap.String("rdma_device", rdmaDevice),
					zap.String("addr", advertiseAddr),
				)
			}
		}
	}
	cfg.Config.RDMADevice = rdmaDevice
	cfg.Config.RDMAAdvertiseAddr = advertiseAddr

	pausingSandboxes, err := utils.NewAdjustableSemaphore(maxPausingInstancesPerNode)
	if err != nil {
		return nil, fmt.Errorf("failed to create pausing sandboxes semaphore: %w", err)
	}

	server := &Server{
		config:            cfg.Config,
		sandboxFactory:    cfg.SandboxFactory,
		info:              cfg.Info,
		proxy:             cfg.Proxy,
		networkPool:       cfg.NetworkPool,
		templateCache:     cfg.TemplateCache,
		devicePool:        cfg.DevicePool,
		persistence:       cfg.Persistence,
		featureFlags:      cfg.FeatureFlags,
		sbxEventsService:  cfg.SbxEventsService,
		startingSandboxes: semaphore.NewWeighted(maxStartingInstancesPerNode),
		pausingSandboxes:  pausingSandboxes,
		peerRegistry:      cfg.PeerRegistry,
		uploadedBuilds:    uploadedBuilds,
		rdmaConfig: rdma.Config{
			SourceBinary: cfg.Config.RDMASourceBinary,
			DestBinary:   cfg.Config.RDMADestBinary,
			Device:       cfg.Config.RDMADevice,
			GIDIndex:     cfg.Config.RDMAGIDIndex,
			HCAPort:      cfg.Config.RDMAHCAPort,
		},
		apiClient:  apiclient.New(cfg.Config.APIBaseURL, cfg.Config.APIAdminToken),
		migrations: make(map[string]*migrationSession),
	}

	meter := cfg.Tel.MeterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	sandboxCreateDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxCreateDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox create duration histogram: %w", err)
	}
	server.sandboxCreateDuration = sandboxCreateDuration

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorSandboxCountMeterName, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(server.sandboxFactory.Sandboxes.Count()))

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox count metric: %w", err)
	}

	return server, nil
}
