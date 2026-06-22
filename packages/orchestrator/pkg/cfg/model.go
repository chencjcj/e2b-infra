package cfg

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const DefaultBusyboxVersion = "1.36.1"

type BuilderConfig struct {
	AllowSandboxInternet   bool          `env:"ALLOW_SANDBOX_INTERNET"   envDefault:"true"`
	DomainName             string        `env:"DOMAIN_NAME"              envDefault:""`
	EnvdTimeout            time.Duration `env:"ENVD_TIMEOUT"             envDefault:"10s"`
	FirecrackerVersionsDir string        `env:"FIRECRACKER_VERSIONS_DIR" envDefault:"/fc-versions"`
	BusyboxVersion         string        `env:"BUSYBOX_VERSION"          envDefault:"1.36.1"`
	HostBusyboxDir         string        `env:"HOST_BUSYBOX_DIR"         envDefault:"/fc-busybox"`
	HostEnvdPath           string        `env:"HOST_ENVD_PATH"           envDefault:"/fc-envd/envd"`
	HostKernelsDir         string        `env:"HOST_KERNELS_DIR"         envDefault:"/fc-kernels"`
	OrchestratorBaseDir    string        `env:"ORCHESTRATOR_BASE_PATH"   envDefault:"/orchestrator"`
	SandboxDir             string        `env:"SANDBOX_DIR"              envDefault:"/fc-vm"`
	SharedChunkCacheDir    string        `env:"SHARED_CHUNK_CACHE_PATH"`
	EnableSharedMemory     bool          `env:"ENABLE_SHARED_MEMORY"     envDefault:"false"`
	TemplatesDir           string        `env:"TEMPLATES_DIR,expand"     envDefault:"${ORCHESTRATOR_BASE_PATH}/build-templates"`

	DefaultCacheDir string `env:"DEFAULT_CACHE_DIR,expand" envDefault:"${ORCHESTRATOR_BASE_PATH}/build"`

	Provider string `env:"PROVIDER" envDefault:"gcp"`

	StorageConfig storage.Config
	NetworkConfig network.Config
}

func makePathsAbsolute(c *BuilderConfig) error {
	for _, item := range []*string{
		&c.DefaultCacheDir,
		&c.FirecrackerVersionsDir,
		&c.HostBusyboxDir,
		&c.HostEnvdPath,
		&c.HostKernelsDir,
		&c.OrchestratorBaseDir,
		&c.StorageConfig.SandboxCacheDir,
		&c.SandboxDir,
		&c.SharedChunkCacheDir,
		&c.StorageConfig.SnapshotCacheDir,
		&c.StorageConfig.TemplateCacheDir,
		&c.TemplatesDir,
	} {
		dir := *item

		if dir == "" {
			continue
		}

		if filepath.IsAbs(dir) {
			continue
		}

		dir, err := filepath.Abs(dir)
		if err != nil {
			return fmt.Errorf("failed to resolve %q to absolute path: %w", *item, err)
		}

		*item = dir
	}

	return nil
}

type Config struct {
	BuilderConfig

	ClickhouseConnectionString string            `env:"CLICKHOUSE_CONNECTION_STRING"`
	ForceStop                  bool              `env:"FORCE_STOP"`
	GRPCPort                   uint16            `env:"GRPC_PORT"                     envDefault:"5008"`
	LaunchDarklyAPIKey         string            `env:"LAUNCH_DARKLY_API_KEY"`
	LocalUploadBaseURL         string            `env:"LOCAL_UPLOAD_BASE_URL"`
	NodeIP                     string            `env:"NODE_IP"                       envDefault:"localhost"`
	NodeLabels                 []string          `env:"NODE_LABELS"                   envSeparator:","`
	OrchestratorLockPath       string            `env:"ORCHESTRATOR_LOCK_PATH"        envDefault:"/orchestrator.lock"`
	NFSProxyLogging            bool              `env:"NFS_PROXY_LOGGING"             envDefault:"false"`
	NFSProxyTracing            bool              `env:"NFS_PROXY_TRACING"             envDefault:"false"`
	NFSProxyMetrics            bool              `env:"NFS_PROXY_METRICS"             envDefault:"true"`
	NFSProxyRecordHandleCalls  bool              `env:"NFS_PROXY_RECORD_HANDLE_CALLS" envDefault:"false"`
	NFSProxyRecordStatCalls    bool              `env:"NFS_PROXY_RECORD_STAT_CALLS"   envDefault:"false"`
	NFSProxyLogLevel           nfs.LogLevel      `env:"NFS_PROXY_LOG_LEVEL"           envDefault:"info"`
	ProxyPort                  uint16            `env:"PROXY_PORT"                    envDefault:"5007"`
	RedisClusterURL            string            `env:"REDIS_CLUSTER_URL"`
	RedisTLSCABase64           string            `env:"REDIS_TLS_CA_BASE64"`
	RedisURL                   string            `env:"REDIS_URL"`
	RedisPoolSize              int               `env:"REDIS_POOL_SIZE"               envDefault:"5"`
	RedisMinIdleConns          int               `env:"REDIS_MIN_IDLE_CONNS"          envDefault:"2"`
	NBDPoolSize                int               `env:"NBD_POOL_SIZE"                 envDefault:"64"`
	Services                   []string          `env:"ORCHESTRATOR_SERVICES"         envDefault:"orchestrator"`
	PersistentVolumeMounts     map[string]string `env:"PERSISTENT_VOLUME_MOUNTS"`

	// RDMA migration config; empty source/dest binary paths disable migration.
	RDMASourceBinary  string `env:"RDMA_SOURCE_BIN"`
	RDMADestBinary    string `env:"RDMA_DEST_BIN"`
	// RDMADevice optionally pins the ibverbs device name (e.g. mlx5_10).
	// If empty, the orchestrator auto-discovers it by scanning
	// /sys/class/infiniband/ and matching against RDMASubnet.
	RDMADevice        string `env:"RDMA_DEVICE"`
	// RDMASubnet is the RoCE network in CIDR form (e.g. 10.253.240.0/24).
	// Used to auto-discover the right RDMA device when devices are named
	// differently across nodes (mlx5_10 here, mlx5_2 there, mlx5_bond_0
	// somewhere else). Highly recommended for fleet deployments — set this
	// once, and every node finds its own RDMA NIC + IP.
	RDMASubnet        string `env:"RDMA_SUBNET"`
	RDMAGIDIndex      uint8  `env:"RDMA_GID_INDEX" envDefault:"3"`
	RDMAHCAPort       uint8  `env:"RDMA_HCA_PORT"  envDefault:"1"`
	// RDMAAdvertiseAddr is the IPv4 address this node advertises in
	// PrepareMigrationSource responses for the destination's TCP connect.
	// Auto-resolved from RDMADevice/RDMASubnet at startup if unset.
	RDMAAdvertiseAddr string `env:"RDMA_ADVERTISE_ADDR"`

	// API hooks — used by the OOM rescue path to ask the API to live-migrate
	// the victim sandbox before falling back to the GCS snapshot path. Both
	// must be set to enable; otherwise the OOM path goes straight to GCS.
	APIBaseURL    string `env:"API_BASE_URL"`
	APIAdminToken string `env:"API_ADMIN_TOKEN"`
}

func (c Config) NodeAddress() *string {
	if c.NodeIP == "localhost" {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", c.NodeIP, c.GRPCPort)

	return &addr
}

func Parse() (Config, error) {
	config, err := env.ParseAsWithOptions[Config](env.Options{
		FuncMap: map[reflect.Type]env.ParserFunc{
			reflect.TypeFor[nfs.LogLevel](): func(s string) (any, error) {
				s = strings.ToLower(s)

				return nfs.Log.ParseLevel(s)
			},
		},
	})
	if err != nil {
		return config, err
	}

	bc := config.BuilderConfig
	if err = makePathsAbsolute(&bc); err != nil {
		return config, err
	}

	config.BuilderConfig = bc

	if config.PersistentVolumeMounts != nil {
		for name, path := range config.PersistentVolumeMounts {
			path = filepath.Clean(path)
			path, err = filepath.Abs(path)
			if err != nil {
				return config, fmt.Errorf("failed to make persistent volume mount %q an absolute path: %w", name, err)
			}

			if _, err := os.Stat(path); err != nil {
				return config, fmt.Errorf("failed to access persistent volume mount %q (%q): %w", name, path, err)
			}

			config.PersistentVolumeMounts[name] = path // store the cleaned path
		}
	}

	return config, nil
}

func ParseBuilder() (BuilderConfig, error) {
	model, err := env.ParseAs[BuilderConfig]()
	if err != nil {
		return BuilderConfig{}, err
	}

	if err = makePathsAbsolute(&model); err != nil {
		return BuilderConfig{}, err
	}

	return model, nil
}
