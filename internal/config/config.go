package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds the complete configuration for the Aqueduct broker.
type Config struct {
	ListenAddr  string          `yaml:"listen_addr"`
	MetricsAddr string          `yaml:"metrics_addr"`
	TLS         TLSConfig       `yaml:"tls"`
	AAL         AALConfig       `yaml:"aal"`
	Transport   TransportConfig `yaml:"transport"`
}

// TLSConfig defines TLS certificate settings.
type TLSConfig struct {
	Generate bool   `yaml:"generate"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// AALConfig defines Append-Only Logging settings.
type AALConfig struct {
	Enabled  bool   `yaml:"enabled"`
	FilePath string `yaml:"file_path"`
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
			Generate: true,
		},
		AAL: AALConfig{
			Enabled:  false,
			FilePath: "",
		},
		Transport: TransportConfig{
			MaxBufSize:  64 * 1024,
			ReadBufSize: 1024,
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
	if v := os.Getenv("AQUEDUCT_AAL_ENABLED"); v != "" {
		cfg.AAL.Enabled = parseBool(v, cfg.AAL.Enabled)
	}
	if v := os.Getenv("AQUEDUCT_AAL_FILE_PATH"); v != "" {
		cfg.AAL.FilePath = v
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
