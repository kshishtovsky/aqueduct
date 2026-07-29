package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the complete configuration for the Aqueduct broker.
type Config struct {
	ListenAddr    string            `yaml:"listen_addr"`
	MetricsAddr   string            `yaml:"metrics_addr"`
	TLS           TLSConfig         `yaml:"tls"`
	AAL           AALConfig         `yaml:"aal"`
	ACL           ACLConfig         `yaml:"acl"`
	Admin         AdminConfig       `yaml:"admin"`
	Broker        BrokerConfig      `yaml:"broker"`
	Transport     TransportConfig   `yaml:"transport"`
	Cluster       ClusterConfig     `yaml:"cluster"`
	Tracing       TracingConfig     `yaml:"tracing"`
	Compression   CompressionConfig `yaml:"compression"`
	WebTransport  WebTransportConfig `yaml:"webtransport"`
}

// AdminConfig defines settings for the gRPC Admin API.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

// TracingConfig defines OpenTelemetry tracing settings.
type TracingConfig struct {
	Enabled     bool   `yaml:"enabled"`
	ServiceName string `yaml:"service_name"`
	Endpoint    string `yaml:"endpoint"`
}

// ClusterConfig holds peer addresses and discovery settings for Direct Mesh Federation.
type ClusterConfig struct {
	Peers     []string        `yaml:"peers"`     // Static peer addresses e.g. ["node-b:4242", "node-c:4242"]
	Discovery DiscoveryConfig `yaml:"discovery"` // Dynamic peer discovery settings
	Mesh      MeshConfig      `yaml:"mesh"`      // Cluster mesh TLS verification settings
}

// MeshConfig configures TLS verification for cluster mesh peer connections.
// InsecureSkipVerify defaults to false (secure by default). Set to true ONLY
// for self-signed dev meshes. Production deployments should keep it false and
// either provide a CAFile or rely on the system CA pool.
type MeshConfig struct {
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // G402 default false; set true ONLY for self-signed dev mesh
	CAFile             string `yaml:"ca_file"`              // PEM file with CA certs for peer verification
}

// DiscoveryConfig defines DNS-based peer discovery for Kubernetes deployments.
type DiscoveryConfig struct {
	Enabled  bool   `yaml:"enabled"`  // enable DNS-based peer discovery
	Type     string `yaml:"type"`     // discovery type: "dns" (only option for MVP)
	Host     string `yaml:"host"`     // headless service FQDN e.g. "aqueduct-headless.default.svc.cluster.local"
	Port     string `yaml:"port"`     // port suffix e.g. "4242" (default: extracted from listen_addr)
	Interval string `yaml:"interval"` // polling interval e.g. "10s" (default: 10s)
}

// TLSConfig defines TLS certificate and mTLS settings.
type TLSConfig struct {
	Generate          bool   `yaml:"generate"`
	CertFile          string `yaml:"cert_file"`
	KeyFile           string `yaml:"key_file"`
	RequireClientCert bool   `yaml:"require_client_cert"`
	ClientCAFile      string `yaml:"client_ca_file"`
}

// AALConfig defines Append-Only Logging and encryption settings.
type AALConfig struct {
	Enabled         bool   `yaml:"enabled"`
	FilePath        string `yaml:"file_path"`
	Key             string `yaml:"key"` // base64 encoded 32-byte key for AES-256-GCM
	MaxFileSize     int64  `yaml:"max_aal_size"`
	RetentionPeriod string `yaml:"retention_period"` // e.g. "24h"
	RetentionSize   int64  `yaml:"retention_size"`   // e.g. 1073741824 (1GB)
}

// ACLRuleConfig defines a single client permission rule.
type ACLRuleConfig struct {
	Client     string `yaml:"client"`
	Topic      string `yaml:"topic"`
	Permission string `yaml:"permission"` // "publish", "subscribe", "all"
}

// ACLConfig defines authorization settings.
type ACLConfig struct {
	Enabled bool            `yaml:"enabled"`
	Default string          `yaml:"default"` // "none", "all"
	Rules   []ACLRuleConfig `yaml:"rules"`
}

// QuotasConfig defines per-tenant rate limiting parameters.
type QuotasConfig struct {
	DefaultPublishRate int                    `yaml:"default_publish_rate"` // default rate limit per tenant (msg/s), 0 = unlimited
	DefaultBurstSize   int                    `yaml:"default_burst_size"`   // default burst size per tenant
	PerClient          map[string]ClientQuota `yaml:"per_client"`           // per-client overrides
}

// ClientQuota defines rate limit for a specific client.
type ClientQuota struct {
	Rate  int `yaml:"rate"`
	Burst int `yaml:"burst"`
}

// BrokerConfig defines async queue size, backpressure isolation policies,
// and coalesced write batching configuration.
type BrokerConfig struct {
	QueueSize          int           `yaml:"queue_size"`
	BackpressurePolicy string        `yaml:"backpressure_policy"` // "drop_oldest", "drop_newest", "disconnect"
	BatchSize          int           `yaml:"batch_size"`          // coalesced write threshold in bytes (default 64KB)
	FlushInterval      time.Duration `yaml:"flush_interval"`      // micro-timer flush interval (default 50µs)
	MaxRetries         int           `yaml:"max_retries"`         // max NACK retries before DLQ (default 3)
	PriorityTTLs       []string      `yaml:"priority_ttls"`       // per-priority TTL array e.g. ["500ms", "5s", "0", "0"]
	Quotas             QuotasConfig  `yaml:"quotas"`
}

// GetPriorityTTLs parses PriorityTTLs strings into a fixed 4-element time.Duration array.
func (b BrokerConfig) GetPriorityTTLs() [4]time.Duration {
	var res [4]time.Duration
	for i, s := range b.PriorityTTLs {
		// #nosec G602 -- bounds-check `if i >= 4 { break }` is below; gosec's data-flow analyzer misses the early-exit.
		if i >= 4 {
			break
		}
		if s == "" || s == "0" || s == "0s" {
			// #nosec G602 -- same as above; i < 4 is guaranteed here.
			res[i] = 0
			continue
		}
		d, err := time.ParseDuration(s)
		if err == nil {
			// #nosec G602 -- same as above.
			res[i] = d
		}
	}
	return res
}

// CompressionConfig defines payload compression settings for batch forwarding.
// Only ZSTD is supported. Compression is applied to batches exceeding MinBatchSize.
type CompressionConfig struct {
	Enabled      bool `yaml:"enabled"`
	MinBatchSize int  `yaml:"min_batch_size"` // bytes, default 1024 (1KB)
	Level        int  `yaml:"level"`          // ZSTD compression level (0=default, 1=fastest, 3=default)
}

// WebTransportConfig configures the optional WebTransport (HTTP/3)
// gateway that lets web browsers connect to the broker on a separate UDP
// port. The gateway reuses the broker's mTLS certificate from cfg.TLS —
// only the listen address is independently configured.
type WebTransportConfig struct {
	Enabled  bool   `yaml:"enabled"`   // if false (default), the gateway is not started
	ListenAddr string `yaml:"listen_addr"` // e.g. ":4433" — must be distinct from cfg.ListenAddr
	PathPrefix string `yaml:"path_prefix"` // URL path for Extended CONNECT; defaults to "/aqueduct/wt"
}

// TransportConfig defines internal buffer limits.
type TransportConfig struct {
	MaxBufSize  int `yaml:"max_buf_size"`
	ReadBufSize int `yaml:"read_buf_size"`
}

// Default returns a Config initialized with safe production/development defaults.
func Default() *Config {
	return &Config{
		ListenAddr:  ":4242",
		MetricsAddr: ":9090",
		TLS: TLSConfig{
			Generate:          true,
			RequireClientCert: false,
		},
		AAL: AALConfig{
			Enabled:         false,
			FilePath:        "",
			Key:             "",
			MaxFileSize:     100 * 1024 * 1024, // 100 MB
			RetentionPeriod: "24h",
			RetentionSize:   1024 * 1024 * 1024, // 1 GB
		},
		ACL: ACLConfig{
			Enabled: false,
			Default: "none",
			Rules:   nil,
		},
		Admin: AdminConfig{
			Enabled: false,
			Addr:    ":9091",
		},
		Broker: BrokerConfig{
			QueueSize:          1024,
			BackpressurePolicy: "drop_oldest",
			BatchSize:          64 * 1024,             // 64 KB
			FlushInterval:      50 * time.Microsecond, // 50 µs
			MaxRetries:         3,                     // 3 NACK retries before DLQ
			Quotas: QuotasConfig{
				DefaultPublishRate: 0,
				DefaultBurstSize:   1000,
			},
		},
		Transport: TransportConfig{
			MaxBufSize:  64 * 1024,
			ReadBufSize: 1024,
		},
		Compression: CompressionConfig{
			Enabled:      false,
			MinBatchSize: 1024,
			Level:        0,
		},
		Tracing: TracingConfig{
			Enabled:     false,
			ServiceName: "aqueduct-broker",
			Endpoint:    "localhost:4317",
		},
		WebTransport: WebTransportConfig{
			Enabled:    false,
			ListenAddr: ":4433",
			PathPrefix: "/aqueduct/wt",
		},
	}
}

// Load loads configuration from a YAML file at path (if non-empty) and applies
// environment variable overrides (AQUEDUCT_*).
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		// #nosec G304 -- path is operator-controlled (-config flag / AQUEDUCT_* env), not from untrusted input.
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read file %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: unmarshal yaml: %w", err)
		}
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

// applyEnvOverrides inspects environment variables starting with AQUEDUCT_
// and overrides matching configuration values.
//
// Split into per-subsystem helpers so each function stays well under
// Sonar's 15-cognitive-complexity threshold.
func applyEnvOverrides(cfg *Config) {
	applyListenEnv(cfg)
	applyTLSEnv(cfg)
	applyAALEnv(cfg)
	applyACLEnv(cfg)
	applyAdminEnv(cfg)
	applyBrokerEnv(cfg)
	applyTransportEnv(cfg)
	applyTracingEnv(cfg)
	applyClusterEnv(cfg)
	applyCompressionEnv(cfg)
	applyWebTransportEnv(cfg)
}

func applyListenEnv(cfg *Config) {
	envString("AQUEDUCT_LISTEN_ADDR", &cfg.ListenAddr)
	envString("AQUEDUCT_METRICS_ADDR", &cfg.MetricsAddr)
}

func applyTLSEnv(cfg *Config) {
	envBool("AQUEDUCT_TLS_GENERATE", &cfg.TLS.Generate)
	envString("AQUEDUCT_TLS_CERT_FILE", &cfg.TLS.CertFile)
	envString("AQUEDUCT_TLS_KEY_FILE", &cfg.TLS.KeyFile)
	envBool("AQUEDUCT_TLS_REQUIRE_CLIENT_CERT", &cfg.TLS.RequireClientCert)
	envString("AQUEDUCT_TLS_CLIENT_CA_FILE", &cfg.TLS.ClientCAFile)
}

func applyAALEnv(cfg *Config) {
	envBool("AQUEDUCT_AAL_ENABLED", &cfg.AAL.Enabled)
	envString("AQUEDUCT_AAL_FILE_PATH", &cfg.AAL.FilePath)
	envString("AQUEDUCT_AAL_KEY", &cfg.AAL.Key)
	envInt64("AQUEDUCT_AAL_MAX_SIZE", &cfg.AAL.MaxFileSize)
}

func applyACLEnv(cfg *Config) {
	envBool("AQUEDUCT_ACL_ENABLED", &cfg.ACL.Enabled)
	envString("AQUEDUCT_ACL_DEFAULT", &cfg.ACL.Default)
}

func applyAdminEnv(cfg *Config) {
	envBool("AQUEDUCT_ADMIN_ENABLED", &cfg.Admin.Enabled)
	envString("AQUEDUCT_ADMIN_ADDR", &cfg.Admin.Addr)
}

// applyBrokerEnv reads AQUEDUCT_BROKER_* env vars into cfg.Broker.
// Cognitive complexity is bounded by extracting each env-var pair into a
// dedicated helper (envString, envPositiveInt, envNonNegativeInt, envDuration).
func applyBrokerEnv(cfg *Config) {
	envString("AQUEDUCT_BROKER_BACKPRESSURE_POLICY", &cfg.Broker.BackpressurePolicy)
	envPositiveInt("AQUEDUCT_BROKER_QUEUE_SIZE", &cfg.Broker.QueueSize)
	envPositiveInt("AQUEDUCT_BROKER_BATCH_SIZE", &cfg.Broker.BatchSize)
	envDuration("AQUEDUCT_BROKER_FLUSH_INTERVAL", &cfg.Broker.FlushInterval)
	envPositiveInt("AQUEDUCT_BROKER_MAX_RETRIES", &cfg.Broker.MaxRetries)
	envNonNegativeInt("AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE", &cfg.Broker.Quotas.DefaultPublishRate)
	envPositiveInt("AQUEDUCT_BROKER_DEFAULT_BURST_SIZE", &cfg.Broker.Quotas.DefaultBurstSize)
}

// envString sets *dst to the value of the given env var if non-empty.
func envString(name string, dst *string) {
	if v := os.Getenv(name); v != "" {
		*dst = v
	}
}

// envPositiveInt parses the env var as int and writes it to *dst if > 0.
func envPositiveInt(name string, dst *int) {
	v := os.Getenv(name)
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err == nil && n > 0 {
		*dst = n
	}
}

// envNonNegativeInt parses the env var as int and writes it to *dst if >= 0.
func envNonNegativeInt(name string, dst *int) {
	v := os.Getenv(name)
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err == nil && n >= 0 {
		*dst = n
	}
}

// envDuration parses the env var as time.Duration and writes it to *dst if > 0.
func envDuration(name string, dst *time.Duration) {
	v := os.Getenv(name)
	if v == "" {
		return
	}
	d, err := time.ParseDuration(v)
	if err == nil && d > 0 {
		*dst = d
	}
}

// envInt64 parses the env var as int64 and writes it to *dst if > 0.
func envInt64(name string, dst *int64) {
	v := os.Getenv(name)
	if v == "" {
		return
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err == nil && n > 0 {
		*dst = n
	}
}

// envBool parses the env var via parseBool and writes it to *dst.
func envBool(name string, dst *bool) {
	if v := os.Getenv(name); v != "" {
		*dst = parseBool(v, *dst)
	}
}

func applyTransportEnv(cfg *Config) {
	envPositiveInt("AQUEDUCT_TRANSPORT_MAX_BUF_SIZE", &cfg.Transport.MaxBufSize)
	envPositiveInt("AQUEDUCT_TRANSPORT_READ_BUF_SIZE", &cfg.Transport.ReadBufSize)
}

func applyTracingEnv(cfg *Config) {
	envBool("AQUEDUCT_TRACING_ENABLED", &cfg.Tracing.Enabled)
	envString("AQUEDUCT_TRACING_SERVICE_NAME", &cfg.Tracing.ServiceName)
	envString("AQUEDUCT_TRACING_ENDPOINT", &cfg.Tracing.Endpoint)
}

func applyClusterEnv(cfg *Config) {
	envBool("AQUEDUCT_CLUSTER_DISCOVERY_ENABLED", &cfg.Cluster.Discovery.Enabled)
	envString("AQUEDUCT_CLUSTER_DISCOVERY_HOST", &cfg.Cluster.Discovery.Host)
	envString("AQUEDUCT_CLUSTER_DISCOVERY_PORT", &cfg.Cluster.Discovery.Port)
	envString("AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL", &cfg.Cluster.Discovery.Interval)
	envBool("AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY", &cfg.Cluster.Mesh.InsecureSkipVerify)
	envString("AQUEDUCT_CLUSTER_MESH_CA_FILE", &cfg.Cluster.Mesh.CAFile)
}

func applyCompressionEnv(cfg *Config) {
	envBool("AQUEDUCT_COMPRESSION_ENABLED", &cfg.Compression.Enabled)
}

func applyWebTransportEnv(cfg *Config) {
	envBool("AQUEDUCT_WEBTRANSPORT_ENABLED", &cfg.WebTransport.Enabled)
	envString("AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR", &cfg.WebTransport.ListenAddr)
	envString("AQUEDUCT_WEBTRANSPORT_PATH_PREFIX", &cfg.WebTransport.PathPrefix)
}

func parseBool(str string, fallback bool) bool {
	str = strings.ToLower(strings.TrimSpace(str))
	switch str {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return fallback
	}
}
