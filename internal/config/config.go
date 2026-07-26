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
	ListenAddr  string            `yaml:"listen_addr"`
	MetricsAddr string            `yaml:"metrics_addr"`
	TLS         TLSConfig         `yaml:"tls"`
	AAL         AALConfig         `yaml:"aal"`
	ACL         ACLConfig         `yaml:"acl"`
	Admin       AdminConfig       `yaml:"admin"`
	Broker      BrokerConfig      `yaml:"broker"`
	Transport   TransportConfig   `yaml:"transport"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Tracing     TracingConfig     `yaml:"tracing"`
	Compression CompressionConfig `yaml:"compression"`
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

// ClusterConfig holds peer addresses for Direct Mesh Federation.
type ClusterConfig struct {
	Peers []string `yaml:"peers"` // Peer addresses e.g. ["node-b:4242", "node-c:4242"]
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
		if i >= 4 {
			break
		}
		if s == "" || s == "0" || s == "0s" {
			res[i] = 0
			continue
		}
		d, err := time.ParseDuration(s)
		if err == nil {
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
	}
}

// Load loads configuration from a YAML file at path (if non-empty) and applies
// environment variable overrides (AQUEDUCT_*).
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
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
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("AQUEDUCT_LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("AQUEDUCT_METRICS_ADDR"); v != "" {
		cfg.MetricsAddr = v
	}
	if v := os.Getenv("AQUEDUCT_TLS_GENERATE"); v != "" {
		cfg.TLS.Generate = parseBool(v, cfg.TLS.Generate)
	}
	if v := os.Getenv("AQUEDUCT_TLS_CERT_FILE"); v != "" {
		cfg.TLS.CertFile = v
	}
	if v := os.Getenv("AQUEDUCT_TLS_KEY_FILE"); v != "" {
		cfg.TLS.KeyFile = v
	}
	if v := os.Getenv("AQUEDUCT_TLS_REQUIRE_CLIENT_CERT"); v != "" {
		cfg.TLS.RequireClientCert = parseBool(v, cfg.TLS.RequireClientCert)
	}
	if v := os.Getenv("AQUEDUCT_TLS_CLIENT_CA_FILE"); v != "" {
		cfg.TLS.ClientCAFile = v
	}
	if v := os.Getenv("AQUEDUCT_AAL_ENABLED"); v != "" {
		cfg.AAL.Enabled = parseBool(v, cfg.AAL.Enabled)
	}
	if v := os.Getenv("AQUEDUCT_AAL_FILE_PATH"); v != "" {
		cfg.AAL.FilePath = v
	}
	if v := os.Getenv("AQUEDUCT_AAL_KEY"); v != "" {
		cfg.AAL.Key = v
	}
	if v := os.Getenv("AQUEDUCT_AAL_MAX_SIZE"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.AAL.MaxFileSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_ACL_ENABLED"); v != "" {
		cfg.ACL.Enabled = parseBool(v, cfg.ACL.Enabled)
	}
	if v := os.Getenv("AQUEDUCT_ACL_DEFAULT"); v != "" {
		cfg.ACL.Default = v
	}
	if v := os.Getenv("AQUEDUCT_ADMIN_ENABLED"); v != "" {
		cfg.Admin.Enabled = parseBool(v, cfg.Admin.Enabled)
	}
	if v := os.Getenv("AQUEDUCT_ADMIN_ADDR"); v != "" {
		cfg.Admin.Addr = v
	}
	if v := os.Getenv("AQUEDUCT_BROKER_QUEUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Broker.QueueSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_BROKER_BACKPRESSURE_POLICY"); v != "" {
		cfg.Broker.BackpressurePolicy = v
	}
	if v := os.Getenv("AQUEDUCT_BROKER_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Broker.BatchSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_BROKER_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Broker.FlushInterval = d
		}
	}
	if v := os.Getenv("AQUEDUCT_BROKER_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Broker.MaxRetries = n
		}
	}
	if v := os.Getenv("AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.Broker.Quotas.DefaultPublishRate = n
		}
	}
	if v := os.Getenv("AQUEDUCT_BROKER_DEFAULT_BURST_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Broker.Quotas.DefaultBurstSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_TRANSPORT_MAX_BUF_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Transport.MaxBufSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_TRANSPORT_READ_BUF_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Transport.ReadBufSize = n
		}
	}
	if v := os.Getenv("AQUEDUCT_TRACING_ENABLED"); v != "" {
		cfg.Tracing.Enabled = parseBool(v, cfg.Tracing.Enabled)
	}
	if v := os.Getenv("AQUEDUCT_TRACING_SERVICE_NAME"); v != "" {
		cfg.Tracing.ServiceName = v
	}
	if v := os.Getenv("AQUEDUCT_TRACING_ENDPOINT"); v != "" {
		cfg.Tracing.Endpoint = v
	}
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
