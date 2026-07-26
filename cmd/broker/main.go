package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/config"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/transport"
)

func main() {
	configFile := flag.String("config", "", "Path to YAML configuration file")
	certFile := flag.String("cert", "", "Path to TLS certificate file")
	keyFile := flag.String("key", "", "Path to TLS private key file")
	aalFile := flag.String("aal", "", "Path to Append-Only Log file")
	addrFlag := flag.String("addr", "", "Broker listen UDP address")
	metricsAddrFlag := flag.String("metrics-addr", "", "Metrics HTTP server address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.Load(*configFile)
	if err != nil {
		logger.Error("failed to load configuration", "err", err)
		os.Exit(1)
	}

	// CLI flag overrides
	if *addrFlag != "" {
		cfg.ListenAddr = *addrFlag
	}
	if *metricsAddrFlag != "" {
		cfg.MetricsAddr = *metricsAddrFlag
	}
	if *certFile != "" {
		cfg.TLS.CertFile = *certFile
		cfg.TLS.Generate = false
	}
	if *keyFile != "" {
		cfg.TLS.KeyFile = *keyFile
		cfg.TLS.Generate = false
	}
	if *aalFile != "" {
		cfg.AAL.Enabled = true
		cfg.AAL.FilePath = *aalFile
	}

	var tlsConf *tls.Config

	if !cfg.TLS.Generate && cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			logger.Error("failed to load TLS certificate and key", "cert", cfg.TLS.CertFile, "key", cfg.TLS.KeyFile, "err", err)
			os.Exit(1)
		}
		tlsConf = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"aqueduct-v1"},
			MinVersion:   tls.VersionTLS13,
		}
		logger.Info("using production TLS certificate", "cert", cfg.TLS.CertFile, "key", cfg.TLS.KeyFile)
	} else {
		logger.Warn("Using ephemeral self-signed certificate. Do not use in production.")
		tlsConf, err = generateSelfSignedTLS()
		if err != nil {
			logger.Error("failed to generate TLS config", "err", err)
			os.Exit(1)
		}
	}

	if cfg.TLS.RequireClientCert {
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		if cfg.TLS.ClientCAFile != "" {
			caPEM, err := os.ReadFile(cfg.TLS.ClientCAFile)
			if err != nil {
				logger.Error("failed to read client CA file", "path", cfg.TLS.ClientCAFile, "err", err)
				os.Exit(1)
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caPEM) {
				logger.Error("failed to parse client CA certificates from PEM", "path", cfg.TLS.ClientCAFile)
				os.Exit(1)
			}
			tlsConf.ClientCAs = caPool
			logger.Info("loaded client CA pool for mTLS verification", "path", cfg.TLS.ClientCAFile)
		} else {
			logger.Info("mTLS enabled: strict client certificate verification required (using system CA pool)")
		}
	}

	if err := metrics.StartServer(cfg.MetricsAddr); err != nil {
		logger.Error("failed to start metrics server", "err", err)
		os.Exit(1)
	}
	logger.Info("metrics server started", "addr", cfg.MetricsAddr)

	routerMetrics := &prometheusMetrics{}
	policy := broker.ParseBackpressurePolicy(cfg.Broker.BackpressurePolicy)
	router := broker.NewRouter(
		routerMetrics,
		broker.WithQueueSize(cfg.Broker.QueueSize),
		broker.WithBackpressurePolicy(policy),
	)
	logger.Info("router initialized", "queue_size", cfg.Broker.QueueSize, "backpressure_policy", policy.String())

	opts := []transport.Option{
		transport.WithLogger(logger),
		transport.WithRouter(router),
		transport.WithMaxBufSize(cfg.Transport.MaxBufSize),
		transport.WithReadBufSize(cfg.Transport.ReadBufSize),
	}

	if cfg.ACL.Enabled {
		defaultPerm := authz.PermNone
		if strings.ToLower(cfg.ACL.Default) == "all" {
			defaultPerm = authz.PermAll
		}
		builder := authz.NewBuilder(defaultPerm)
		for _, r := range cfg.ACL.Rules {
			var perm authz.Permission
			switch strings.ToLower(r.Permission) {
			case "publish":
				perm = authz.PermPublish
			case "subscribe":
				perm = authz.PermSubscribe
			case "all":
				perm = authz.PermAll
			}
			builder.Allow(r.Client, r.Topic, perm)
		}
		opts = append(opts, transport.WithAuthz(builder.Build()))
		logger.Info("authorization ACL engine enabled")
	}

	if cfg.AAL.Enabled && cfg.AAL.FilePath != "" {
		var aalKey []byte
		if cfg.AAL.Key != "" {
			keyBytes, err := base64.StdEncoding.DecodeString(cfg.AAL.Key)
			if err == nil && len(keyBytes) == 32 {
				aalKey = keyBytes
			} else if len(cfg.AAL.Key) == 32 {
				aalKey = []byte(cfg.AAL.Key)
			} else {
				logger.Error("AAL encryption key must be 32 bytes (base64 encoded or raw)")
				os.Exit(1)
			}
		}

		aalLog, err := aal.OpenEncrypted(cfg.AAL.FilePath, aalKey)
		if err != nil {
			logger.Error("failed to open AAL file", "path", cfg.AAL.FilePath, "err", err)
			os.Exit(1)
		}
		if len(aalKey) > 0 {
			logger.Info("encrypted append-only logging enabled (AES-256-GCM)", "path", cfg.AAL.FilePath)
		} else {
			logger.Info("append-only logging enabled", "path", cfg.AAL.FilePath)
		}
		opts = append(opts, transport.WithAAL(aalLog))
	}

	b := transport.New(opts...)

	b.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		logger.Info("publish received", "stream_id", frame.StreamID, "payload_len", frame.PayloadLen)
		return nil, nil
	})

	b.Handle(protocol.CmdSubscribe, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		logger.Info("subscribe received", "stream_id", frame.StreamID, "payload_len", frame.PayloadLen)
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Listen(ctx, cfg.ListenAddr, tlsConf); err != nil {
		logger.Error("failed to start listener", "err", err)
		os.Exit(1)
	}

	logger.Info("broker started", "addr", b.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.Info("received signal, shutting down", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := b.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
		os.Exit(1)
	}

	logger.Info("broker stopped cleanly")
}

type prometheusMetrics struct{}

func (m *prometheusMetrics) OnPublish(topic string) {
	metrics.MessagesPublished.WithLabelValues(topic).Inc()
}

func (m *prometheusMetrics) OnDeliver(topic string) {
	metrics.MessagesDelivered.WithLabelValues(topic).Inc()
}

func (m *prometheusMetrics) SetActiveSubscribers(n float64) {
	metrics.ActiveSubscribers.Set(n)
}


func generateSelfSignedTLS() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate rsa key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{Organization: []string{"Aqueduct Ephemeral"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"aqueduct-v1"},
		MinVersion:   tls.VersionTLS13,
	}, nil
}
