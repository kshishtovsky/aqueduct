package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigDefault(t *testing.T) {
	cfg := Default()
	if cfg.ListenAddr != ":4242" {
		t.Errorf("expected default ListenAddr :4242, got %q", cfg.ListenAddr)
	}
	if cfg.MetricsAddr != ":9090" {
		t.Errorf("expected default MetricsAddr :9090, got %q", cfg.MetricsAddr)
	}
	if !cfg.TLS.Generate {
		t.Errorf("expected default TLS.Generate true, got false")
	}
	if cfg.AAL.Enabled {
		t.Errorf("expected default AAL.Enabled false, got true")
	}
	if cfg.Transport.MaxBufSize != 64*1024 {
		t.Errorf("expected default MaxBufSize 64KB, got %d", cfg.Transport.MaxBufSize)
	}
}

func TestConfigLoadYAML(t *testing.T) {
	yamlContent := `
listen_addr: ":5252"
metrics_addr: ":9191"
tls:
  generate: false
  cert_file: "/path/cert.pem"
  key_file: "/path/key.pem"
aal:
  enabled: true
  file_path: "/path/aal.log"
transport:
  max_buf_size: 131072
  read_buf_size: 2048
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp yaml: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.ListenAddr != ":5252" {
		t.Errorf("expected ListenAddr :5252, got %q", cfg.ListenAddr)
	}
	if cfg.MetricsAddr != ":9191" {
		t.Errorf("expected MetricsAddr :9191, got %q", cfg.MetricsAddr)
	}
	if cfg.TLS.Generate {
		t.Errorf("expected TLS.Generate false, got true")
	}
	if cfg.TLS.CertFile != "/path/cert.pem" {
		t.Errorf("expected CertFile /path/cert.pem, got %q", cfg.TLS.CertFile)
	}
	if !cfg.AAL.Enabled || cfg.AAL.FilePath != "/path/aal.log" {
		t.Errorf("unexpected AAL config: %+v", cfg.AAL)
	}
	if cfg.Transport.MaxBufSize != 131072 || cfg.Transport.ReadBufSize != 2048 {
		t.Errorf("unexpected Transport config: %+v", cfg.Transport)
	}
}

func TestConfigEnvOverrides(t *testing.T) {
	t.Setenv("AQUEDUCT_LISTEN_ADDR", ":6262")
	t.Setenv("AQUEDUCT_METRICS_ADDR", ":9292")
	t.Setenv("AQUEDUCT_TLS_GENERATE", "false")
	t.Setenv("AQUEDUCT_TLS_CERT_FILE", "/env/cert.pem")
	t.Setenv("AQUEDUCT_TLS_KEY_FILE", "/env/key.pem")
	t.Setenv("AQUEDUCT_AAL_ENABLED", "true")
	t.Setenv("AQUEDUCT_AAL_FILE_PATH", "/env/aal.log")
	t.Setenv("AQUEDUCT_TRANSPORT_MAX_BUF_SIZE", "262144")
	t.Setenv("AQUEDUCT_TRANSPORT_READ_BUF_SIZE", "4096")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.ListenAddr != ":6262" {
		t.Errorf("expected ListenAddr :6262, got %q", cfg.ListenAddr)
	}
	if cfg.MetricsAddr != ":9292" {
		t.Errorf("expected MetricsAddr :9292, got %q", cfg.MetricsAddr)
	}
	if cfg.TLS.Generate {
		t.Errorf("expected TLS.Generate false")
	}
	if cfg.TLS.CertFile != "/env/cert.pem" || cfg.TLS.KeyFile != "/env/key.pem" {
		t.Errorf("unexpected TLS files: %+v", cfg.TLS)
	}
	if !cfg.AAL.Enabled || cfg.AAL.FilePath != "/env/aal.log" {
		t.Errorf("unexpected AAL: %+v", cfg.AAL)
	}
	if cfg.Transport.MaxBufSize != 262144 || cfg.Transport.ReadBufSize != 4096 {
		t.Errorf("unexpected Transport: %+v", cfg.Transport)
	}
}

func TestConfigLoadInvalidFile(t *testing.T) {
	_, err := Load("/non_existent_path/config.yaml")
	if err == nil {
		t.Error("expected error loading non-existent file, got nil")
	}

	tmpFile := filepath.Join(t.TempDir(), "invalid.yaml")
	_ = os.WriteFile(tmpFile, []byte("invalid: yaml: ["), 0644)
	_, err = Load(tmpFile)
	if err == nil {
		t.Error("expected error unmarshaling invalid yaml, got nil")
	}
}
