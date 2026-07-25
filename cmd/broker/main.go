package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/transport"
)

func main() {
	certFile := flag.String("cert", "", "Path to TLS certificate file")
	keyFile := flag.String("key", "", "Path to TLS private key file")
	aalFile := flag.String("aal", "", "Path to Append-Only Log file")
	addrFlag := flag.String("addr", ":4242", "Broker listen UDP address")
	metricsAddrFlag := flag.String("metrics-addr", ":9090", "Metrics HTTP server address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	var tlsConf *tls.Config
	var err error

	if *certFile != "" && *keyFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			logger.Error("failed to load TLS certificate and key", "cert", *certFile, "key", *keyFile, "err", err)
			os.Exit(1)
		}
		tlsConf = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"aqueduct-v1"},
			MinVersion:   tls.VersionTLS13,
		}
		logger.Info("using production TLS certificate", "cert", *certFile, "key", *keyFile)
	} else {
		logger.Warn("Using ephemeral self-signed certificate. Do not use in production.")
		tlsConf, err = generateSelfSignedTLS()
		if err != nil {
			logger.Error("failed to generate TLS config", "err", err)
			os.Exit(1)
		}
	}

	// Start metrics HTTP server on :9090 or flag/env override.
	metricsAddr := *metricsAddrFlag
	if envAddr := os.Getenv("METRICS_ADDR"); envAddr != "" && metricsAddr == ":9090" {
		metricsAddr = envAddr
	}
	if err := metrics.StartServer(metricsAddr); err != nil {
		logger.Error("failed to start metrics server", "err", err)
		os.Exit(1)
	}
	logger.Info("metrics server started", "addr", metricsAddr)

	// Create router with Prometheus metrics.
	routerMetrics := &prometheusMetrics{}
	router := broker.NewRouter(routerMetrics)

	opts := []transport.Option{
		transport.WithLogger(logger),
		transport.WithRouter(router),
	}

	if *aalFile != "" {
		aalLog, err := aal.Open(*aalFile)
		if err != nil {
			logger.Error("failed to open AAL file", "path", *aalFile, "err", err)
			os.Exit(1)
		}
		logger.Info("append-only logging enabled", "path", *aalFile)
		opts = append(opts, transport.WithAAL(aalLog))
	}

	b := transport.New(opts...)

	// Publish/Subscribe are handled by the built-in router.
	// The handler system remains available for custom commands.
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

	addr := *addrFlag
	if envAddr := os.Getenv("BROKER_ADDR"); envAddr != "" && addr == ":4242" {
		addr = envAddr
	}

	if err := b.Listen(ctx, addr, tlsConf); err != nil {
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

	fmt.Println("broker stopped gracefully")
}

// prometheusMetrics adapts the global Prometheus counters to the RouterMetrics interface.
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
		return nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{Organization: []string{"Aqueduct"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"aqueduct-v1"},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

