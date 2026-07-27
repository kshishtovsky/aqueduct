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
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/admin"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/cluster"
	"github.com/kshishtovsky/aqueduct/internal/config"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/transport"
	"github.com/quic-go/quic-go"
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

	var aalKey []byte
	if cfg.AAL.Enabled && cfg.AAL.Key != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(cfg.AAL.Key)
		if err == nil && len(keyBytes) == 32 {
			aalKey = keyBytes
		} else if len(cfg.AAL.Key) == 32 {
			aalKey = []byte(cfg.AAL.Key)
		}
	}

	routerMetrics := &prometheusMetrics{}
	policy := broker.ParseBackpressurePolicy(cfg.Broker.BackpressurePolicy)
	routerOpts := []broker.RouterOption{
		broker.WithQueueSize(cfg.Broker.QueueSize),
		broker.WithBackpressurePolicy(policy),
		broker.WithAALPath(cfg.AAL.FilePath, aalKey),
		broker.WithBatchSize(cfg.Broker.BatchSize),
		broker.WithFlushInterval(cfg.Broker.FlushInterval),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize cluster peer federation if peers are configured.
	var pm *cluster.PeerManager
	if len(cfg.Cluster.Peers) > 0 || cfg.Cluster.Discovery.Enabled {
		// Build the peer TLS config based on operator-provided settings.
		// Default: skip verification (allows self-signed dev mesh with TLS encryption).
		// Production: set cluster.tls.insecure_skip_verify: false and provide a CA pool.
		//
		// SECURITY: InsecureSkipVerify disables host-name verification of the peer
		// certificate, leaving the mesh open to MITM attacks if network traffic is
		// intercepted. In production, point RootCAs at a CA bundle that signs all
		// peer certificates (e.g. internal cluster CA) and set the flag to false.
		// #nosec G402 -- operator-controlled cluster TLS; setting gated by config.Mesh.InsecureSkipVerify.
		peerTLS := &tls.Config{
			InsecureSkipVerify: cfg.Cluster.Mesh.InsecureSkipVerify,
			NextProtos:         []string{"aqueduct-mesh"},
		}
		peerQUIC := &quic.Config{MaxIdleTimeout: 30 * time.Second}

		if cfg.Cluster.Mesh.InsecureSkipVerify {
			logger.Warn("cluster mesh TLS verification disabled (cluster.tls.insecure_skip_verify=true). " +
				"This is vulnerable to MITM attacks. Use a CA-signed certificate in production.")
		} else {
			if cfg.Cluster.Mesh.CAFile != "" {
				caPEM, err := os.ReadFile(cfg.Cluster.Mesh.CAFile)
				if err != nil {
					logger.Error("failed to read cluster mesh CA file", "path", cfg.Cluster.Mesh.CAFile, "err", err)
					os.Exit(1)
				}
				caPool := x509.NewCertPool()
				if !caPool.AppendCertsFromPEM(caPEM) {
					logger.Error("failed to parse cluster mesh CA certificates from PEM", "path", cfg.Cluster.Mesh.CAFile)
					os.Exit(1)
				}
				peerTLS.RootCAs = caPool
				logger.Info("cluster mesh TLS using custom CA pool", "ca_file", cfg.Cluster.Mesh.CAFile)
			} else {
				systemPool, err := x509.SystemCertPool()
				if err != nil || systemPool == nil {
					systemPool = x509.NewCertPool()
				}
				peerTLS.RootCAs = systemPool
				logger.Info("cluster mesh TLS using system CA pool")
			}
		}

		// Start with static peers (may be empty if discovery-only mode).
		pm = cluster.NewWithLogger(ctx, cfg.Cluster.Peers, peerTLS, peerQUIC, logger)
		routerOpts = append(routerOpts, broker.WithPeerForwarder(pm))

		if len(cfg.Cluster.Peers) > 0 {
			logger.Info("cluster federation enabled (static peers)", "peers", cfg.Cluster.Peers)
		}

		// Start DNS-based peer discovery if configured.
		if cfg.Cluster.Discovery.Enabled && cfg.Cluster.Discovery.Host != "" {
			discInterval, err := time.ParseDuration(cfg.Cluster.Discovery.Interval)
			if err != nil || discInterval <= 0 {
				discInterval = 10 * time.Second
			}
			discPort := cfg.Cluster.Discovery.Port
			if discPort == "" {
				// Extract port from listen_addr if not explicitly set.
				if _, p, err := net.SplitHostPort(cfg.ListenAddr); err == nil && p != "" {
					discPort = p
				} else {
					discPort = "4242"
				}
			}
			discCfg := cluster.DiscoveryConfig{
				Enabled:  true,
				Host:     cfg.Cluster.Discovery.Host,
				Port:     discPort,
				Interval: discInterval,
			}
			disc := cluster.NewDiscovery(cluster.DefaultResolver{}, discCfg, pm, logger)
			disc.Start(ctx)
			logger.Info("dns peer discovery started",
				"host", cfg.Cluster.Discovery.Host,
				"port", discPort,
				"interval", discInterval,
			)
		}
	}

	router := broker.NewRouter(routerMetrics, routerOpts...)
	logger.Info("router initialized", "queue_size", cfg.Broker.QueueSize, "backpressure_policy", policy.String())

	opts := []transport.Option{
		transport.WithLogger(logger),
		transport.WithRouter(router),
		transport.WithMaxBufSize(cfg.Transport.MaxBufSize),
		transport.WithReadBufSize(cfg.Transport.ReadBufSize),
	}

	var authzEngine *authz.Engine
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
		authzEngine = builder.Build()
		opts = append(opts, transport.WithAuthz(authzEngine))
		logger.Info("authorization ACL engine enabled")
	}

	if cfg.AAL.Enabled && cfg.AAL.FilePath != "" {
		var aalKey2 []byte
		if cfg.AAL.Key != "" {
			keyBytes, err := base64.StdEncoding.DecodeString(cfg.AAL.Key)
			if err == nil && len(keyBytes) == 32 {
				aalKey2 = keyBytes
			} else if len(cfg.AAL.Key) == 32 {
				aalKey2 = []byte(cfg.AAL.Key)
			} else {
				logger.Error("AAL encryption key must be 32 bytes (base64 encoded or raw)")
				os.Exit(1)
			}
		}

		aalLog, err := aal.OpenEncrypted(cfg.AAL.FilePath, aalKey2)
		if err != nil {
			logger.Error("failed to open AAL file", "path", cfg.AAL.FilePath, "err", err)
			os.Exit(1)
		}
		if len(aalKey2) > 0 {
			logger.Info("encrypted append-only logging enabled (AES-256-GCM)", "path", cfg.AAL.FilePath)
		} else {
			logger.Info("append-only logging enabled", "path", cfg.AAL.FilePath)
		}
		opts = append(opts, transport.WithAAL(aalLog), transport.WithAALReplay(cfg.AAL.FilePath, aalKey2))
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

	var adminServer *admin.Server
	if cfg.Admin.Enabled {
		if authzEngine == nil {
			authzEngine = authz.NewEngine(nil, authz.PermAll)
			b.SetAuthzEngine(authzEngine)
		}
		adminServer = admin.NewServer(router.QuotaManager(), authzEngine, admin.WithLogger(logger))
		if err := adminServer.Start(cfg.Admin.Addr, tlsConf); err != nil {
			logger.Error("failed to start admin server", "addr", cfg.Admin.Addr, "err", err)
			os.Exit(1)
		}
		logger.Info("admin gRPC server started", "addr", cfg.Admin.Addr)
	}

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

	if adminServer != nil {
		adminServer.Stop()
	}

	if err := b.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
		os.Exit(1)
	}

	if pm != nil {
		pm.Close()
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

func (m *prometheusMetrics) OnRateLimited(clientID string) {
	metrics.MessagesRateLimited.WithLabelValues(clientID).Inc()
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
