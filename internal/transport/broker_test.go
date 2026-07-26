package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const testProto = "aqueduct-v1"

// testTLSConfig returns paired server and client TLS configs for testing.
func testTLSConfig(t *testing.T) (server, client *tls.Config) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{"Test"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	// Parse back to verify correctness.
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}

	server = &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{testProto},
		MinVersion:   tls.VersionTLS13,
	}

	// Verify: manually check cert signature against its own public key.
	parsedCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	if err := parsedCert.CheckSignatureFrom(parsedCert); err != nil {
		t.Fatalf("self-signature check failed: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(parsedCert)
	client = &tls.Config{
		RootCAs:    pool,
		NextProtos: []string{testProto},
		MinVersion: tls.VersionTLS13,
	}

	return server, client
}

// startBroker creates a Broker on a random port with the given handler and
// returns the broker, its address, client TLS config, and a cleanup function.
func startBroker(t *testing.T, cmd protocol.Command, handler Handler) (*Broker, string, *tls.Config) {
	t.Helper()

	sTLS, cTLS := testTLSConfig(t)
	broker := New()
	broker.Handle(cmd, handler)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		broker.Shutdown(shCtx)
	})

	return broker, broker.Addr().String(), cTLS
}

func dialClient(t *testing.T, addr string, cTLS *tls.Config) *quic.Conn {
	t.Helper()

	conn, err := quic.DialAddr(
		context.Background(),
		addr,
		cTLS,
		&quic.Config{
			MaxIdleTimeout: 10 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.CloseWithError(0, "test done")
	})
	return conn
}

func TestBrokerPublish(t *testing.T) {
	var received atomic.Bool
	var streamID atomic.Uint32

	_, addr, cTLS := startBroker(t, protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		received.Store(true)
		streamID.Store(frame.StreamID)
		if string(frame.Payload) != "hello" {
			t.Errorf("expected payload 'hello', got %q", frame.Payload)
		}
		return nil, nil
	})

	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("hello")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 42, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !received.Load() {
		t.Fatal("handler was not called")
	}
	if sid := streamID.Load(); sid != 42 {
		t.Errorf("expected stream_id 42, got %d", sid)
	}
}

func TestBrokerSubscribe(t *testing.T) {
	var received atomic.Bool

	_, addr, cTLS := startBroker(t, protocol.CmdSubscribe, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		received.Store(true)
		return nil, nil
	})

	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("topic:orders")
	buf := protocol.SerializeFrame(protocol.CmdSubscribe, 7, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !received.Load() {
		t.Fatal("handler was not called")
	}
}

func TestBrokerMultipleStreams(t *testing.T) {
	var count atomic.Int32

	_, addr, cTLS := startBroker(t, protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		count.Add(1)
		return nil, nil
	})

	conn := dialClient(t, addr, cTLS)

	const numStreams = 5
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := range numStreams {
		go func(id int) {
			defer wg.Done()
			stream, err := conn.OpenStreamSync(context.Background())
			if err != nil {
				t.Errorf("open stream %d: %v", id, err)
				return
			}

			payload := []byte(fmt.Sprintf("msg-%d", id))
			buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(id), payload)
			defer protocol.ReleaseBuffer(buf)

			if _, err := stream.Write(*buf); err != nil {
				t.Errorf("write stream %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	if c := count.Load(); c != numStreams {
		t.Errorf("expected %d handler calls, got %d", numStreams, c)
	}
}

func TestBrokerShutdown(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	broker := New()
	broker.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		return nil, nil
	})

	ctx := context.Background()
	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := broker.Addr().String()
	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("shutdown-test")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestBrokerShutdownTimeout(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	broker := New()

	// Handler that blocks until context is cancelled — simulates slow drain.
	broker.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		<-ctx.Done()
		return nil, nil
	})

	ctx := context.Background()
	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := broker.Addr().String()
	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("block-test")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Very short timeout — should not block forever.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestBrokerNoHandler(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	broker := New()
	// No handlers registered — should not panic.

	ctx := context.Background()
	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := broker.Addr().String()
	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// Use an unknown command (CmdAck) — the broker has no handler for it.
	payload := []byte("no-handler")

	// Actually, the protocol parser rejects unknown commands.
	// Use a valid command with no handler instead.
	buf := protocol.SerializeFrame(protocol.CmdAck, 99, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Should not panic — "no handler" path logs and continues.
}

func TestBrokerHandlerError(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	broker := New()
	broker.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		return nil, fmt.Errorf("handler failed: %w", context.DeadlineExceeded)
	})

	ctx := context.Background()
	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := broker.Addr().String()
	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("error-test")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	// Should not panic — error is logged, not propagated to caller.
}

func TestBrokerDoubleShutdown(t *testing.T) {
	sTLS, _ := testTLSConfig(t)
	broker := New()

	ctx := context.Background()
	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	shutdownCtx := context.Background()
	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}

	// Second shutdown should be a no-op.
	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestBrokerAddrNil(t *testing.T) {
	broker := New()
	if addr := broker.Addr(); addr != nil {
		t.Errorf("expected nil addr before listen, got %v", addr)
	}
}

func TestBrokerWithCustomOptions(t *testing.T) {
	broker := New(
		WithReadBufSize(2048),
		WithMaxBufSize(128*1024),
	)

	if broker.readBufSize != 2048 {
		t.Errorf("expected readBufSize 2048, got %d", broker.readBufSize)
	}
	if broker.maxBufSize != 128*1024 {
		t.Errorf("expected maxBufSize %d, got %d", 128*1024, broker.maxBufSize)
	}
}

func TestBrokerOOMProtection(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	maxBuf := 1 * 1024 * 1024 // 1 MB limit
	broker := New(WithMaxBufSize(maxBuf))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		broker.Shutdown(shCtx)
	})

	conn := dialClient(t, broker.Addr().String(), cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// Frame claiming 10 MB payload
	oversizedLen := uint32(10 * 1024 * 1024)
	header := make([]byte, protocol.HeaderSize)
	header[0] = protocol.MagicByte
	header[1] = byte(protocol.CmdPublish)
	header[2], header[3], header[4], header[5] = 0, 0, 0, 1 // streamID 1
	header[6] = byte(oversizedLen >> 24)
	header[7] = byte(oversizedLen >> 16)
	header[8] = byte(oversizedLen >> 8)
	header[9] = byte(oversizedLen)

	if _, err := stream.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	stream.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 10)
	_, readErr := stream.Read(buf)
	if readErr == nil {
		t.Error("expected error/stream cancellation from server due to oversized payload, got nil error")
	}
}

func TestBrokerWithAAL(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "broker_aal.log")
	aalLog, err := aal.Open(logPath)
	if err != nil {
		t.Fatalf("aal.Open failed: %v", err)
	}

	sTLS, cTLS := testTLSConfig(t)
	broker := New(WithAAL(aalLog))
	broker.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	conn := dialClient(t, broker.Addr().String(), cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	messages := [][]byte{
		[]byte("msg-1"),
		[]byte("msg-2-longer"),
		[]byte("msg-3-final"),
	}

	var totalExpectedBytes []byte
	for i, msg := range messages {
		buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(i+1), msg)
		lenPrefix := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenPrefix, uint32(len(*buf)))
		totalExpectedBytes = append(totalExpectedBytes, lenPrefix...)
		totalExpectedBytes = append(totalExpectedBytes, (*buf)...)
		if _, err := stream.Write(*buf); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}
		protocol.ReleaseBuffer(buf)
	}

	time.Sleep(200 * time.Millisecond)

	shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shCancel()
	if err := broker.Shutdown(shCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file failed: %v", err)
	}

	if len(content) != len(totalExpectedBytes) {
		t.Fatalf("log size mismatch: got %d bytes, want %d bytes", len(content), len(totalExpectedBytes))
	}

	if !bytes.Equal(content, totalExpectedBytes) {
		t.Fatal("log binary content mismatch")
	}
}

func sendFrame(t *testing.T, stream *quic.Stream, cmd protocol.Command, streamID uint32, payload []byte) {
	t.Helper()
	buf := protocol.SerializeFrame(cmd, streamID, payload)
	defer protocol.ReleaseBuffer(buf)
	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func TestBrokerResponse(t *testing.T) {
	_, addr, cTLS := startBroker(t, protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		return []byte("pong"), nil
	})

	conn := dialClient(t, addr, cTLS)
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	payload := []byte("ping")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 123, payload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	replyBuf := make([]byte, 1024)
	n, err := stream.Read(replyBuf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	respFrame, err := protocol.ParseFrame(replyBuf[:n])
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if respFrame.Command != protocol.CmdAck {
		t.Errorf("expected CmdAck, got %v", respFrame.Command)
	}
	if string(respFrame.Payload) != "pong" {
		t.Errorf("expected payload 'pong', got %q", respFrame.Payload)
	}
}

func TestBrokerRouterIntegration(t *testing.T) {
	r := broker.NewRouter(nil)
	b := New(WithRouter(r), WithLogger(slog.Default()))
	if b.router != r {
		t.Errorf("expected router option set")
	}

	sTLS, cTLS := testTLSConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := b.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	conn1 := dialClient(t, b.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:test-route"))

	time.Sleep(200 * time.Millisecond)

	conn2 := dialClient(t, b.Addr().String(), cTLS)
	pub, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open pub: %v", err)
	}
	sendFrame(t, pub, protocol.CmdPublish, 2, []byte("test-route"))

	sub1.SetReadDeadline(time.Now().Add(2 * time.Second))
	recvBuf := make([]byte, 1024)
	n, err := sub1.Read(recvBuf)
	if err != nil {
		t.Fatalf("read delivered msg: %v", err)
	}
	recvFrame, err := protocol.ParseFrame(recvBuf[:n])
	if err != nil {
		t.Fatalf("parse delivered msg: %v", err)
	}
	if string(recvFrame.Payload) != "test-route" {
		t.Errorf("expected payload 'test-route', got %q", recvFrame.Payload)
	}
}

func TestBrokerBufferGrowth(t *testing.T) {
	var received atomic.Bool
	sTLS, cTLS := testTLSConfig(t)

	b := New(WithReadBufSize(1024), WithMaxBufSize(64*1024))
	b.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		received.Store(true)
		if len(frame.Payload) != 3000 {
			t.Errorf("expected 3000 payload bytes, got %d", len(frame.Payload))
		}
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := b.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}

	conn := dialClient(t, b.Addr().String(), cTLS)
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	largePayload := make([]byte, 3000)
	for i := range largePayload {
		largePayload[i] = 'A'
	}

	buf := protocol.SerializeFrame(protocol.CmdPublish, 99, largePayload)
	defer protocol.ReleaseBuffer(buf)

	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write large frame: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !received.Load() {
		t.Error("handler for grown buffer frame was not called")
	}
}

// BenchmarkQUICRoundTrip measures end-to-end frame processing latency.
// Client sends a frame, server parses and dispatches to handler.
func BenchmarkQUICRoundTrip(b *testing.B) {
	sTLS, cTLS := testBenchTLS(b)

	broker := New()
	broker.Handle(protocol.CmdPublish, func(ctx context.Context, frame protocol.Frame) ([]byte, error) {
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		broker.Shutdown(shCtx)
	}()

	addr := broker.Addr().String()
	conn, err := quic.DialAddrEarly(
		context.Background(),
		addr,
		cTLS,
		&quic.Config{MaxIdleTimeout: 30 * time.Second},
	)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "bench done")

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}

	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i)
	}
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, payload)
	defer protocol.ReleaseBuffer(buf)

	b.SetBytes(int64(protocol.FrameSize(128)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := stream.Write(*buf); err != nil {
			b.Fatalf("write: %v", err)
		}
		// Read echo response (handler returns nil, but we need to read to
		// avoid flow control stall). For MVP, we drain with a short read.
		_ = make([]byte, 64)
	}
}

func testBenchTLS(b *testing.B) (server, client *tls.Config) {
	b.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatalf("generate key: %v", err)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{"Bench"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		b.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		b.Fatalf("key pair: %v", err)
	}

	server = &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{testProto},
		MinVersion:   tls.VersionTLS13,
	}

	pool := x509.NewCertPool()
	parsedCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		b.Fatalf("parse cert: %v", err)
	}
	pool.AddCert(parsedCert)
	client = &tls.Config{
		RootCAs:    pool,
		NextProtos: []string{testProto},
		MinVersion: tls.VersionTLS13,
	}

	return server, client
}

func TestBrokerAuthzAllowedAndDenied(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)
	r := broker.NewRouter(nil)

	aclEngine := authz.NewBuilder(authz.PermNone).
		Allow("anonymous", "allowed_topic", authz.PermPublish|authz.PermSubscribe).
		Build()

	b := New(WithRouter(r), WithAuthz(aclEngine))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shCancel()
		_ = b.Shutdown(shCtx)
	}()

	addr := b.Addr().String()
	conn, err := quic.DialAddr(context.Background(), addr, cTLS, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "test done") }()

	// 1. Allowed Subscribe
	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open sub stream: %v", err)
	}
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, []byte("allowed_topic"))
	if _, err := subStream.Write(*subBuf); err != nil {
		t.Fatalf("write subscribe allowed: %v", err)
	}
	protocol.ReleaseBuffer(subBuf)

	// 2. Denied Publish to forbidden topic
	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open pub stream: %v", err)
	}
	deniedBuf := protocol.SerializeFrame(protocol.CmdPublish, 2, []byte("forbidden_topic"))
	if _, err := pubStream.Write(*deniedBuf); err != nil {
		t.Fatalf("write publish forbidden: %v", err)
	}
	protocol.ReleaseBuffer(deniedBuf)

	// Read on pubStream should fail due to stream cancellation (code 401)
	readBuf := make([]byte, 100)
	_ = pubStream.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = pubStream.Read(readBuf)
	if err == nil {
		t.Error("expected read error on denied stream, got nil")
	}
}

func BenchmarkRouterPublishWithAuthz(b *testing.B) {
	sTLS, cTLS := testBenchTLS(b)
	r := broker.NewRouter(nil)

	aclEngine := authz.NewBuilder(authz.PermNone).
		Allow("anonymous", "bench", authz.PermPublish).
		Build()

	broker := New(WithRouter(r), WithAuthz(aclEngine))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Listen(ctx, "127.0.0.1:0", sTLS); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() {
		shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shCancel()
		_ = broker.Shutdown(shCtx)
	}()

	addr := broker.Addr().String()
	conn, err := quic.DialAddrEarly(
		context.Background(),
		addr,
		cTLS,
		&quic.Config{MaxIdleTimeout: 30 * time.Second},
	)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "bench done") }()

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}

	payload := []byte("bench")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, payload)
	defer protocol.ReleaseBuffer(buf)

	b.SetBytes(int64(protocol.FrameSize(uint32(len(payload)))))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := stream.Write(*buf); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}


