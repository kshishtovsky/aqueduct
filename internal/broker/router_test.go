package broker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/compress"
	"github.com/kshishtovsky/aqueduct/internal/mem"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const testProto = "aqueduct-v1"

func genTLS(t testing.TB) (server, client *tls.Config) {
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

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}

	server = &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{testProto},
		MinVersion:   tls.VersionTLS13,
	}

	parsedCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
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

func dialQUIC(t testing.TB, addr string, cTLS *tls.Config) *quic.Conn {
	t.Helper()

	conn, err := quic.DialAddr(
		context.Background(),
		addr,
		cTLS,
		&quic.Config{MaxIdleTimeout: 10 * time.Second},
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.CloseWithError(0, "test done")
	})
	return conn
}

func sendFrame(t testing.TB, stream *quic.Stream, cmd protocol.Command, streamID uint32, payload []byte) {
	t.Helper()
	buf := protocol.SerializeFrame(cmd, streamID, payload)
	defer protocol.ReleaseBuffer(buf)
	if _, err := stream.Write(*buf); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// TestRouterPublishOneToTwo verifies that one publisher's message is received
// by two subscribers on the same topic via real QUIC streams.
func TestRouterPublishOneToTwo(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil)
	defer router.Close()

	quicConf := &quic.Config{Allow0RTT: true, MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Accept connections and register streams into the router.
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// Subscriber 1
	conn1 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:orders"))

	// Subscriber 2
	conn2 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub2, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub2: %v", err)
	}
	sendFrame(t, sub2, protocol.CmdSubscribe, 2, []byte("topic:orders"))

	// Allow subscribers to be processed.
	time.Sleep(500 * time.Millisecond)

	// Publish a message on the "orders" topic.
	payload := []byte("orders")
	pubFrame := protocol.Frame{
		Command:    protocol.CmdPublish,
		StreamID:   0,
		PayloadLen: uint32(len(payload)),
		Payload:    payload,
	}
	_ = router.Publish(ctx, pubFrame)

	// Read from subscriber 1.
	sub1.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf1 := make([]byte, 1024)
	n1, err := sub1.Read(buf1)
	if err != nil {
		t.Fatalf("sub1 read: %v", err)
	}
	recv1, err := protocol.ParseFrame(buf1[:n1])
	if err != nil {
		t.Fatalf("sub1 parse: %v", err)
	}
	if string(recv1.Payload) != "orders" {
		t.Errorf("sub1: expected 'orders', got %q", recv1.Payload)
	}

	// Read from subscriber 2.
	sub2.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf2 := make([]byte, 1024)
	n2, err := sub2.Read(buf2)
	if err != nil {
		t.Fatalf("sub2 read: %v", err)
	}
	recv2, err := protocol.ParseFrame(buf2[:n2])
	if err != nil {
		t.Fatalf("sub2 parse: %v", err)
	}
	if string(recv2.Payload) != "orders" {
		t.Errorf("sub2: expected 'orders', got %q", recv2.Payload)
	}
}

// TestRouterAsyncSlowConsumerDropOldest tests 3 subscribers (2 fast, 1 slow).
// Fast subscribers receive all messages without delay; slow subscriber drops oldest without blocking publisher.
func TestRouterAsyncSlowConsumerDropOldest(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil, WithQueueSize(10), WithBackpressurePolicy(PolicyDropOldest))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// Fast sub 1
	conn1 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:fast-topic"))

	// Fast sub 2
	conn2 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub2, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub2: %v", err)
	}
	sendFrame(t, sub2, protocol.CmdSubscribe, 2, []byte("topic:fast-topic"))

	// Slow sub 3
	conn3 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub3, err := conn3.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub3: %v", err)
	}
	sendFrame(t, sub3, protocol.CmdSubscribe, 3, []byte("topic:fast-topic"))

	time.Sleep(300 * time.Millisecond)

	// Publish 100 messages rapidly
	start := time.Now()
	for i := 0; i < 100; i++ {
		pubFrame := protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: 10,
			Payload:    []byte("fast-topic"),
		}
		if err := router.Publish(ctx, pubFrame); err != nil {
			t.Fatalf("publish error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Publisher must finish in less than 50ms despite slow subscriber!
	if elapsed > 50*time.Millisecond {
		t.Errorf("publisher was blocked by slow subscriber! took %v", elapsed)
	}

	// Verify fast subscribers read messages cleanly
	sub1.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	if _, err := sub1.Read(buf); err != nil {
		t.Errorf("fast sub1 read failed: %v", err)
	}

	sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := sub2.Read(buf); err != nil {
		t.Errorf("fast sub2 read failed: %v", err)
	}
}

// TestRouterAsyncSlowConsumerDisconnect tests automatic disconnect policy on overflow.
func TestRouterAsyncSlowConsumerDisconnect(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil, WithQueueSize(5), WithBackpressurePolicy(PolicyDisconnect))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrame(buf[:n])
		if err != nil {
			return
		}
		_ = router.Subscribe(ctx, stream, frame)
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:disco-topic"))
	time.Sleep(300 * time.Millisecond)

	// Publish 50 messages to overflow small queue (size 5)
	for i := 0; i < 50; i++ {
		_ = router.Publish(ctx, protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: 11,
			Payload:    []byte("disco-topic"),
		})
	}

	time.Sleep(100 * time.Millisecond)

	// Verify subscriber was disconnected (countActive drops to 0)
	if router.countActiveLocked() != 0 {
		t.Errorf("expected 0 active subscribers after disconnect overflow, got %d", router.countActiveLocked())
	}
}

// TestRouterExtractTopic verifies topic parsing from payload.
func TestRouterExtractTopic(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
		wantErr bool
	}{
		{"valid", "topic:orders", "orders", false},
		{"nested", "topic:a/b/c", "a/b/c", false},
		{"empty payload", "", "", true},
		{"missing prefix", "orders", "", true},
		{"empty topic", "topic:", "", true},
		{"short", "top", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTopic([]byte(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Errorf("extractTopic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractTopic() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRouterUnsubscribe verifies that unsubscribed streams don't receive messages.
func TestRouterUnsubscribe(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		for {
			_, err := conn.AcceptStream(ctx)
			if err != nil {
				return
			}
		}
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	router := NewRouter(nil)
	defer router.Close()

	_ = router.Subscribe(ctx, stream, protocol.Frame{
		Command:    protocol.CmdSubscribe,
		StreamID:   1,
		PayloadLen: 10,
		Payload:    []byte("topic:test"),
	})

	if router.countActiveLocked() != 1 {
		t.Fatalf("expected 1 active, got %d", router.countActiveLocked())
	}

	router.Unsubscribe(1)
	if router.countActiveLocked() != 0 {
		t.Fatalf("expected 0 active after unsubscribe, got %d", router.countActiveLocked())
	}
}

// mockMetrics records calls for verification.
type mockMetrics struct {
	published   atomic.Int64
	delivered   atomic.Int64
	subs        atomic.Int64
	rateLimited atomic.Int64
}

func (m *mockMetrics) OnPublish(topic string) { m.published.Add(1) }
func (m *mockMetrics) OnDeliver(topic string) { m.delivered.Add(1) }
func (m *mockMetrics) SetActiveSubscribers(n float64) {
	m.subs.Store(int64(n))
}
func (m *mockMetrics) OnRateLimited(clientID string) { m.rateLimited.Add(1) }

// TestRouterMetrics verifies that metrics callbacks fire correctly.
func TestRouterMetrics(t *testing.T) {
	mm := &mockMetrics{}
	router := NewRouter(mm)
	defer router.Close()

	sTLS, cTLS := genTLS(t)
	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					buf := make([]byte, 1024)
					n, err := stream.Read(buf)
					if err != nil {
						return
					}
					frame, err := protocol.ParseFrame(buf[:n])
					if err != nil {
						return
					}
					if frame.Command == protocol.CmdSubscribe {
						_ = router.Subscribe(ctx, stream, frame)
					}
				}
			}()
		}
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sendFrame(t, stream, protocol.CmdSubscribe, 1, []byte("topic:metrics"))
	time.Sleep(500 * time.Millisecond)

	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 7,
		Payload:    []byte("metrics"),
	})

	if mm.published.Load() != 1 {
		t.Errorf("expected 1 OnPublish, got %d", mm.published.Load())
	}

	time.Sleep(200 * time.Millisecond)
	if mm.delivered.Load() != 1 {
		t.Errorf("expected 1 OnDeliver, got %d", mm.delivered.Load())
	}
}

// TestRouterPublishToEmptyTopic verifies publish to a topic with no subscribers.
func TestRouterPublishToEmptyTopic(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	err := router.Publish(context.Background(), protocol.Frame{
		Command:    protocol.CmdPublish,
		Payload:    []byte("empty-topic"),
		PayloadLen: 11,
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// TestRouterPublishOversizedPayload verifies the max payload guard.
func TestRouterPublishOversizedPayload(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	oversized := make([]byte, maxPayloadSize+1)
	err := router.Publish(context.Background(), protocol.Frame{
		Command:    protocol.CmdPublish,
		Payload:    oversized,
		PayloadLen: uint32(len(oversized)),
	})
	if err == nil {
		t.Error("expected error for oversized payload")
	}
}

// BenchmarkRouterPublishAsync measures publisher latency for 1, 5, and 10 subscribers.
func BenchmarkRouterPublishAsync(b *testing.B) {
	for _, numSubs := range []int{1, 5, 10} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			sTLS, cTLS := genTLS(b)
			quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
			ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
			if err != nil {
				b.Fatalf("listen: %v", err)
			}
			defer ln.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			router := NewRouter(nil)
			defer router.Close()

			go func() {
				for {
					conn, err := ln.Accept(ctx)
					if err != nil {
						return
					}
					go func() {
						for {
							stream, err := conn.AcceptStream(ctx)
							if err != nil {
								return
							}
							go func(s *quic.Stream) {
								buf := make([]byte, 1024)
								n, err := s.Read(buf)
								if err != nil {
									return
								}
								frame, err := protocol.ParseFrame(buf[:n])
								if err != nil {
									return
								}
								if frame.Command == protocol.CmdSubscribe {
									_ = router.Subscribe(ctx, s, frame)
								}
							}(stream)
						}
					}()
				}
			}()

			// Register subscribers.
			for i := range numSubs {
				conn := dialQUIC(b, ln.Addr().String(), cTLS)
				stream, err := conn.OpenStreamSync(ctx)
				if err != nil {
					b.Fatalf("open sub %d: %v", i, err)
				}
				sendFrame(b, stream, protocol.CmdSubscribe, uint32(i+1), []byte("topic:bench"))
			}
			time.Sleep(300 * time.Millisecond)

			topic := []byte("bench")
			pubFrame := protocol.Frame{
				Command:    protocol.CmdPublish,
				PayloadLen: uint32(len(topic)),
				Payload:    topic,
			}

			b.SetBytes(int64(protocol.FrameSize(uint32(len(topic)))))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = router.Publish(ctx, pubFrame)
			}
		})
	}
}

// BenchmarkRouterPublishWithAAL measures publish performance with AAL enabled.
func BenchmarkRouterPublishWithAAL(b *testing.B) {
	logPath := filepath.Join(b.TempDir(), "aal_bench.log")
	aalLog, err := aal.Open(logPath)
	if err != nil {
		b.Fatalf("aal.Open failed: %v", err)
	}
	defer aalLog.Close()

	router := NewRouter(nil)
	defer router.Close()

	topic := []byte("bench")
	frame := protocol.Frame{
		Command:    protocol.CmdPublish,
		StreamID:   1,
		PayloadLen: uint32(len(topic)),
		Payload:    topic,
	}

	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, topic)
	defer protocol.ReleaseBuffer(buf)

	b.SetBytes(int64(len(*buf)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := aalLog.WriteFrame(*buf); err != nil {
			b.Fatalf("WriteFrame failed: %v", err)
		}
		_ = router.Publish(context.Background(), frame)
	}
}

// TestWildcardRouting verifies + and # wildcard pattern matching in router.
func TestWildcardRouting(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil)
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// Sub 1: Wildcard "+" pattern (sensor/+/temp)
	conn1 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:sensor/+/temp"))

	// Sub 2: Wildcard "#" pattern (sensor/#)
	conn2 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub2, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub2: %v", err)
	}
	sendFrame(t, sub2, protocol.CmdSubscribe, 2, []byte("topic:sensor/#"))

	time.Sleep(300 * time.Millisecond)

	// Publish to sensor/room1/temp (matches both "+" and "#")
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 17,
		Payload:    []byte("sensor/room1/temp"),
	})

	// Sub 1 must receive message
	sub1.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf1 := make([]byte, 1024)
	n1, err := sub1.Read(buf1)
	if err != nil {
		t.Fatalf("sub1 (+) read: %v", err)
	}
	f1, _ := protocol.ParseFrame(buf1[:n1])
	if string(f1.Payload) != "sensor/room1/temp" {
		t.Errorf("sub1 (+): expected 'sensor/room1/temp', got %q", f1.Payload)
	}

	// Sub 2 must receive message
	sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf2 := make([]byte, 1024)
	n2, err := sub2.Read(buf2)
	if err != nil {
		t.Fatalf("sub2 (#) read: %v", err)
	}
	f2, _ := protocol.ParseFrame(buf2[:n2])
	if string(f2.Payload) != "sensor/room1/temp" {
		t.Errorf("sub2 (#): expected 'sensor/room1/temp', got %q", f2.Payload)
	}

	// Publish to sensor/room1/humidity (matches "#" but NOT "+")
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 21,
		Payload:    []byte("sensor/room1/humidity"),
	})

	// Sub 2 (#) must receive humidity
	n2, err = sub2.Read(buf2)
	if err != nil {
		t.Fatalf("sub2 (#) read humidity: %v", err)
	}
	f2, _ = protocol.ParseFrame(buf2[:n2])
	if string(f2.Payload) != "sensor/room1/humidity" {
		t.Errorf("sub2 (#): expected 'sensor/room1/humidity', got %q", f2.Payload)
	}
}

// TestMessageTTLExpiration verifies lazy expiration of messages in queue.
func TestMessageTTLExpiration(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil)
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrame(buf[:n])
		if err != nil {
			return
		}
		_ = router.Subscribe(ctx, stream, frame)
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:ttl-topic"))
	time.Sleep(300 * time.Millisecond)

	// Publish message with expired TTL (-50ms)
	ttlPayload := []byte("ttl:-50:ttl-topic")
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(ttlPayload)),
		Payload:    ttlPayload,
	})

	sub.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 1024)
	_, err = sub.Read(buf)
	if err == nil {
		t.Error("expected deadline exceeded for expired TTL message, but read succeeded")
	}
}

// BenchmarkRouterPublishWithWildcards measures publish latency with 10 wildcard subscribers.
func BenchmarkRouterPublishWithWildcards(b *testing.B) {
	sTLS, cTLS := genTLS(b)
	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil)
	defer router.Close()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// Register 10 wildcard subscribers
	for i := range 10 {
		conn := dialQUIC(b, ln.Addr().String(), cTLS)
		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			b.Fatalf("open wildcard sub %d: %v", i, err)
		}
		sendFrame(b, stream, protocol.CmdSubscribe, uint32(i+1), []byte("topic:sensor/+/temp/#"))
	}
	time.Sleep(300 * time.Millisecond)

	topic := []byte("sensor/room1/temp/sub1")
	pubFrame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(topic)),
		Payload:    topic,
	}

	for i := 0; i < b.N; i++ {
		_ = router.Publish(ctx, pubFrame)
	}
}

// mockPeerForwarder records calls for cluster mesh forwarding tests.
type mockPeerForwarder struct {
	forwardCalls atomic.Int32
	active       atomic.Int32
}

func (m *mockPeerForwarder) Forward(rawBuf []byte, addForwardedBit bool) {
	m.forwardCalls.Add(1)
}

func (m *mockPeerForwarder) ActivePeers() int {
	return int(m.active.Load())
}

// TestRouterPublishFromPeer verifies that a mesh-forwarded frame is delivered to local subscribers
// but does NOT trigger peer re-forwarding (storm protection at the router level).
func TestRouterPublishFromPeer(t *testing.T) {
	sTLS, cTLS := genTLS(t)
	mpf := &mockPeerForwarder{}
	mpf.active.Store(2)

	router := NewRouter(nil, WithPeerForwarder(mpf))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrame(buf[:n])
		if err != nil {
			return
		}
		_ = router.Subscribe(ctx, stream, frame)
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:peer-topic"))
	time.Sleep(300 * time.Millisecond)

	// Simulate a MeshForwarded frame received from a peer node.
	forwardedPayload := []byte("peer-topic")
	forwardedFrame := protocol.Frame{
		Command:    protocol.SetForwarded(protocol.CmdPublish),
		StreamID:   0,
		PayloadLen: uint32(len(forwardedPayload)),
		Payload:    forwardedPayload,
	}

	if err := router.PublishFromPeer(ctx, forwardedFrame); err != nil {
		t.Fatalf("PublishFromPeer: %v", err)
	}

	// Verify local subscriber received the message
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	readBuf := make([]byte, 1024)
	n, err := sub.Read(readBuf)
	if err != nil {
		t.Fatalf("sub read: %v", err)
	}
	f, _ := protocol.ParseFrame(readBuf[:n])
	if string(f.Payload) != "peer-topic" {
		t.Errorf("expected 'peer-topic', got %q", f.Payload)
	}

	// Verify MeshForwarded bit is stripped before delivery
	if protocol.IsForwarded(f.Command) {
		t.Error("expected MeshForwarded bit to be stripped before local delivery")
	}

	// Storm protection: PeerForwarder.Forward must NOT have been called
	if calls := mpf.forwardCalls.Load(); calls != 0 {
		t.Errorf("expected 0 peer forward calls from PublishFromPeer, got %d", calls)
	}
}

// TestRouterPublishWithPeerForwarder verifies that Publish() forwards to peers when
// there are active peer connections. We subscribe a local sub first to prevent the
// early return (no subscribers) before peer forwarding is reached.
func TestRouterPublishWithPeerForwarder(t *testing.T) {
	sTLS, cTLS := genTLS(t)
	mpf := &mockPeerForwarder{}
	mpf.active.Store(2)

	router := NewRouter(nil, WithPeerForwarder(mpf))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 10 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrame(buf[:n])
		if err != nil {
			return
		}
		_ = router.Subscribe(ctx, stream, frame)
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:forward-test"))
	time.Sleep(300 * time.Millisecond)

	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 12,
		Payload:    []byte("forward-test"),
	})

	// Verify Forward was called for peers
	if calls := mpf.forwardCalls.Load(); calls == 0 {
		t.Error("expected Publish to call Forward for active peers")
	}
}

// TestDurableSubscriptionsReconnect tests durable subscriber reconnection and AAL backfill replay.
func TestDurableSubscriptionsReconnect(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "durable_aal.log")
	aalLog, err := aal.Open(logPath)
	if err != nil {
		t.Fatalf("aal.Open failed: %v", err)
	}
	defer aalLog.Close()

	sTLS, cTLS := genTLS(t)
	router := NewRouter(nil, WithAALPath(logPath, nil))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// 1. Initial durable subscription
	conn1 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:durable-topic:durable:client-a:0"))
	time.Sleep(200 * time.Millisecond)

	// Sub1 disconnects
	sub1.Close()

	// 2. Publish 20 messages to durable-topic while Sub1 is offline
	for i := 0; i < 20; i++ {
		payload := []byte("durable-topic")
		buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(i+1), payload)
		if err := aalLog.WriteFrame(*buf); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
		protocol.ReleaseBuffer(buf)
		_ = router.Publish(ctx, protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: uint32(len(payload)),
			Payload:    payload,
		})
	}
	_ = aalLog.Sync()

	// 3. Sub1 reconnects asking for offset 0 backfill
	conn2 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub2, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub2: %v", err)
	}
	sendFrame(t, sub2, protocol.CmdSubscribe, 2, []byte("topic:durable-topic:durable:client-a:0"))

	// 4. Verify sub2 receives backfilled messages
	receivedCount := 0
	sub2.SetReadDeadline(time.Now().Add(3 * time.Second))
	readBuf := make([]byte, 4096)
	off := 0
	for receivedCount < 20 {
		n, err := sub2.Read(readBuf[off:])
		if err != nil {
			t.Fatalf("sub2 backfill read failed: %v", err)
		}
		off += n
		consumed := 0
		for consumed < off {
			if off-consumed < protocol.HeaderSize {
				break
			}
			f, parseErr := protocol.ParseFrame(readBuf[consumed:off])
			if parseErr != nil {
				break
			}
			if string(f.Payload) == "durable-topic" {
				receivedCount++
			}
			consumed += f.Size()
		}
		copy(readBuf, readBuf[consumed:off])
		off -= consumed
	}

	if receivedCount != 20 {
		t.Errorf("expected 20 backfilled messages, got %d", receivedCount)
	}
}

// TestConsumerACK verifies consumer ACK offset tracking.
func TestConsumerACK(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	router.AckOffset("service-a", "orders", 150)
	got := router.GetConsumerOffset("service-a", "orders")
	if got != 150 {
		t.Errorf("expected consumer offset 150, got %d", got)
	}
}

// TestPublishBatchToSubscriber verifies that a CmdPublishBatch frame with
// multiple sub-messages reaches subscribers correctly.
func TestPublishBatchToSubscriber(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil)
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		for {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func(s *quic.Stream) {
				buf := make([]byte, 4096)
				n, err := s.Read(buf)
				if err != nil {
					return
				}
				frame, err := protocol.ParseFrame(buf[:n])
				if err != nil {
					return
				}
				if frame.Command == protocol.CmdSubscribe {
					_ = router.Subscribe(ctx, s, frame)
				}
			}(stream)
		}
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:batch-topic"))
	time.Sleep(300 * time.Millisecond)

	// Build a batch of 5 sub-frames, each with the batch topic as payload
	var subFrames []*[]byte
	for i := range 5 {
		payload := []byte("batch-topic")
		f := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), payload)
		subFrames = append(subFrames, f)
	}

	// Concatenate all sub-frames into batch payload
	totalLen := 0
	for _, f := range subFrames {
		totalLen += len(*f)
	}
	batchPayload := make([]byte, 0, totalLen)
	for _, f := range subFrames {
		batchPayload = append(batchPayload, *f...)
	}
	for _, f := range subFrames {
		protocol.ReleaseBuffer(f)
	}

	// Publish batch
	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(batchPayload)),
		Payload:    batchPayload,
	}
	if err := router.PublishBatch(ctx, batchFrame); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}

	// Sub should receive all 5 messages (they may be coalesced into one write)
	received := 0
	sub.SetReadDeadline(time.Now().Add(3 * time.Second))
	readBuf := make([]byte, 4096)
	off := 0
	for received < 5 {
		n, err := sub.Read(readBuf[off:])
		if err != nil {
			break
		}
		off += n
		consumed := 0
		for consumed < off {
			if off-consumed < protocol.HeaderSize {
				break
			}
			f, parseErr := protocol.ParseFrame(readBuf[consumed:off])
			if parseErr != nil {
				break
			}
			received++
			consumed += f.Size()
		}
		copy(readBuf, readBuf[consumed:off])
		off -= consumed
	}
	if received != 5 {
		t.Errorf("expected 5 received messages, got %d", received)
	}
}

// TestPublishBatchEmpty verifies PublishBatch with no subscribers.
func TestPublishBatchEmpty(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	subPayload := []byte("no-subs")
	subFrame := protocol.SerializeFrame(protocol.CmdPublish, 0, subPayload)
	defer protocol.ReleaseBuffer(subFrame)

	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(*subFrame)),
		Payload:    *subFrame,
	}
	if err := router.PublishBatch(context.Background(), batchFrame); err != nil {
		t.Errorf("PublishBatch to empty topic should succeed: %v", err)
	}
}

// BenchmarkBatchPublish measures throughput for 100k messages in batches of 100.
// Target: 1M+ RPS, 0 allocs/op on hot path.
func BenchmarkBatchPublish(b *testing.B) {
	router := NewRouter(nil)
	defer router.Close()

	// Build sub-frames for the batch (100 per batch, each 128 bytes payload)
	const batchSize = 100
	const msgSize = 128

	var batchParts []*[]byte
	totalBatchBytes := 0
	for i := range batchSize {
		payload := make([]byte, msgSize)
		payload[0] = byte(i)
		f := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), payload)
		batchParts = append(batchParts, f)
		totalBatchBytes += len(*f)
	}

	batchPayload := make([]byte, 0, totalBatchBytes)
	for _, f := range batchParts {
		batchPayload = append(batchPayload, *f...)
	}
	for _, f := range batchParts {
		protocol.ReleaseBuffer(f)
	}

	totalPayloadBytes := int64(totalBatchBytes)

	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(batchPayload)),
		Payload:    batchPayload,
	}

	b.SetBytes(totalPayloadBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = router.PublishBatch(context.Background(), batchFrame)
	}
}

// BenchmarkPublishSingleVsBatch compares single publish vs batch publish throughput.
func BenchmarkPublishSingleVsBatch(b *testing.B) {
	router := NewRouter(nil)
	defer router.Close()

	const msgSize = 128
	payload := make([]byte, msgSize)

	singleFrame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(payload)),
		Payload:    payload,
	}

	b.Run("single", func(b *testing.B) {
		b.SetBytes(int64(protocol.FrameSize(uint32(len(payload)))))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = router.Publish(context.Background(), singleFrame)
		}
	})

	// Batch of 100 identical messages
	const batchSize = 100
	var batchParts []*[]byte
	totalBatchBytes := 0
	for i := range batchSize {
		f := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), payload)
		batchParts = append(batchParts, f)
		totalBatchBytes += len(*f)
	}
	batchPayload := make([]byte, 0, totalBatchBytes)
	for _, f := range batchParts {
		batchPayload = append(batchPayload, *f...)
	}
	for _, f := range batchParts {
		protocol.ReleaseBuffer(f)
	}

	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(batchPayload)),
		Payload:    batchPayload,
	}

	b.Run("batch_100", func(b *testing.B) {
		b.SetBytes(int64(totalBatchBytes))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = router.PublishBatch(context.Background(), batchFrame)
		}
	})
}

// BenchmarkDurablePublish measures publish latency with topic offset tracking.
func BenchmarkDurablePublish(b *testing.B) {
	router := NewRouter(nil)
	defer router.Close()

	topic := []byte("orders")
	frame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(topic)),
		Payload:    topic,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = router.Publish(context.Background(), frame)
	}
}

// TestNackRedelivery verifies that a NACK'd message is redelivered to the subscriber.
func TestNackRedelivery(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil, WithMaxRetries(3))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 1, []byte("topic:nack-test"))
	time.Sleep(300 * time.Millisecond)

	// Publish one message
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 9,
		Payload:    []byte("nack-test"),
	})

	// Read the first delivery
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf1 := make([]byte, 1024)
	n1, err := sub.Read(buf1)
	if err != nil {
		t.Fatalf("first delivery read: %v", err)
	}
	f1, _ := protocol.ParseFrame(buf1[:n1])
	if string(f1.Payload) != "nack-test" {
		t.Fatalf("expected 'nack-test', got %q", f1.Payload)
	}

	// Simulate NACK via NackByStream (offset 1 = first message)
	time.Sleep(100 * time.Millisecond)
	router.NackByStream(1, 1)

	// Read the redelivery
	time.Sleep(200 * time.Millisecond)
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf2 := make([]byte, 1024)
	n2, err := sub.Read(buf2)
	if err != nil {
		t.Fatalf("redelivery read: %v", err)
	}
	f2, _ := protocol.ParseFrame(buf2[:n2])
	if string(f2.Payload) != "nack-test" {
		t.Errorf("redelivery: expected 'nack-test', got %q", f2.Payload)
	}
}

// TestPoisonPillToDLQ verifies that a message NACK'd max_retries times moves to __dlq__ topic.
func TestPoisonPillToDLQ(t *testing.T) {
	sTLS, cTLS := genTLS(t)

	router := NewRouter(nil, WithMaxRetries(3))
	defer router.Close()

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track delivered offsets to identify redeliveries vs DLQ
	topicHits := atomic.Int32{}
	dlqHits := atomic.Int32{}

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	// Subscribe to original topic
	conn1 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub1, err := conn1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub1: %v", err)
	}
	sendFrame(t, sub1, protocol.CmdSubscribe, 1, []byte("topic:nack-dlq"))
	time.Sleep(200 * time.Millisecond)

	// Subscribe to DLQ topic
	conn2 := dialQUIC(t, ln.Addr().String(), cTLS)
	sub2, err := conn2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub2: %v", err)
	}
	sendFrame(t, sub2, protocol.CmdSubscribe, 2, []byte("topic:__dlq__nack-dlq"))
	time.Sleep(300 * time.Millisecond)

	// Publish one message
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 9,
		Payload:    []byte("nack-dlq"),
	})

	// Sub1 reads first delivery
	sub1.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, err := sub1.Read(buf)
	if err != nil {
		t.Fatalf("sub1 first read: %v", err)
	}
	frame, _ := protocol.ParseFrame(buf[:n])
	if string(frame.Payload) != "nack-dlq" {
		t.Fatalf("expected 'nack-dlq', got %q", frame.Payload)
	}
	topicHits.Add(1)

	// NACK 3 times (max_retries = 3) via NackByStream
	for i := 0; i < 3; i++ {
		router.NackByStream(1, 1)
		time.Sleep(200 * time.Millisecond)

		// Read the redelivery
		sub1.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err = sub1.Read(buf)
		if err == nil {
			frame, _ = protocol.ParseFrame(buf[:n])
			if string(frame.Payload) == "nack-dlq" {
				topicHits.Add(1)
			}
		}
	}

	// After 3 NACKs, sub2 (DLQ subscriber) should have received the message
	time.Sleep(300 * time.Millisecond)
	sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = sub2.Read(buf)
	if err != nil {
		t.Fatalf("sub2 (DLQ) read failed: %v. topicHits=%d, dlqHits=%d", err, topicHits.Load(), dlqHits.Load())
	}
	dlqFrame, _ := protocol.ParseFrame(buf[:n])
	if string(dlqFrame.Payload) != "__dlq__nack-dlq" {
		t.Errorf("DLQ: expected '__dlq__nack-dlq' (DLQ topic), got %q", dlqFrame.Payload)
	}
	dlqHits.Add(1)

	if dlqHits.Load() != 1 {
		t.Errorf("expected 1 DLQ delivery, got %d", dlqHits.Load())
	}
}

// TestNackUnknownOffset verifies that NACK for an unknown offset does not panic.
func TestNackUnknownOffset(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	// NACK for offset 99999 on a stream with no subscribers should not panic
	router.NackByStream(99999, 99999)
}

// TestNackByStream verifies NackByStream routes to the correct subscriber.
func TestNackByStream(t *testing.T) {
	router := NewRouter(nil)
	defer router.Close()

	sTLS, cTLS := genTLS(t)
	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrame(buf[:n])
		if err != nil {
			return
		}
		if frame.Command == protocol.CmdSubscribe {
			_ = router.Subscribe(ctx, stream, frame)
		}
	}()

	conn := dialQUIC(t, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub: %v", err)
	}
	sendFrame(t, sub, protocol.CmdSubscribe, 42, []byte("topic:nack-stream-test"))
	time.Sleep(200 * time.Millisecond)

	// Verify we can find the topic by stream ID
	topic, found := router.TopicOfStream(42)
	if !found {
		t.Fatal("topic not found for stream 42")
	}
	if topic != "nack-stream-test" {
		t.Errorf("expected 'nack-stream-test', got %q", topic)
	}

	// NACK on stream 42 should not panic
	router.NackByStream(42, 1)

	// NACK on unregistered stream should be a no-op
	router.NackByStream(999, 1)
}

// mockCompressionPeerForwarder captures forwarded data for compression benchmarks.
type mockCompressionPeerForwarder struct {
	active         atomic.Int32
	forwardedBytes atomic.Int64
}

func (m *mockCompressionPeerForwarder) Forward(rawBuf []byte, addForwardedBit bool) {
	m.forwardedBytes.Add(int64(len(rawBuf)))
}

func (m *mockCompressionPeerForwarder) ActivePeers() int {
	return int(m.active.Load())
}

// BenchmarkBatchPublishWithCompression measures throughput with ZSTD compression.
// Uses a mock peer forwarder to trigger the compression path.
// Expected: 3-5x reduction in wire bytes, slightly higher ns/op due to CPU cost.
func BenchmarkBatchPublishWithCompression(b *testing.B) {
	slab := mem.New()
	engine := compress.NewZstdEngine(slab)

	mpf := &mockCompressionPeerForwarder{}
	mpf.active.Store(2)

	router := NewRouter(nil, WithCompression(engine, 512), WithPeerForwarder(mpf))
	defer router.Close()

	const batchSize = 100
	const msgSize = 128

	var batchParts []*[]byte
	totalBatchBytes := 0
	for i := range batchSize {
		payload := make([]byte, msgSize)
		payload[0] = byte(i)
		f := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), payload)
		batchParts = append(batchParts, f)
		totalBatchBytes += len(*f)
	}

	batchPayload := make([]byte, 0, totalBatchBytes)
	for _, f := range batchParts {
		batchPayload = append(batchPayload, *f...)
	}
	for _, f := range batchParts {
		protocol.ReleaseBuffer(f)
	}

	totalPayloadBytes := int64(totalBatchBytes)

	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(batchPayload)),
		Payload:    batchPayload,
	}

	b.SetBytes(totalPayloadBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = router.PublishBatch(context.Background(), batchFrame)
	}
}

// BenchmarkBatchPublishNoCompressionSamePeerCount measures throughput WITHOUT compression
// but with the same active peer count for fair comparison.
func BenchmarkBatchPublishNoCompressionSamePeerCount(b *testing.B) {
	mpf := &mockCompressionPeerForwarder{}
	mpf.active.Store(2)

	router := NewRouter(nil, WithPeerForwarder(mpf))
	defer router.Close()

	const batchSize = 100
	const msgSize = 128

	var batchParts []*[]byte
	totalBatchBytes := 0
	for i := range batchSize {
		payload := make([]byte, msgSize)
		payload[0] = byte(i)
		f := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), payload)
		batchParts = append(batchParts, f)
		totalBatchBytes += len(*f)
	}

	batchPayload := make([]byte, 0, totalBatchBytes)
	for _, f := range batchParts {
		batchPayload = append(batchPayload, *f...)
	}
	for _, f := range batchParts {
		protocol.ReleaseBuffer(f)
	}

	totalPayloadBytes := int64(totalBatchBytes)

	batchFrame := protocol.Frame{
		Command:    protocol.CmdPublishBatch,
		PayloadLen: uint32(len(batchPayload)),
		Payload:    batchPayload,
	}

	b.SetBytes(totalPayloadBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = router.PublishBatch(context.Background(), batchFrame)
	}
}

// BenchmarkNackHandling measures the performance of NACK processing.
func BenchmarkNackHandling(b *testing.B) {
	sTLS, cTLS := genTLS(b)
	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil, WithMaxRetries(3))
	defer router.Close()

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func(s *quic.Stream) {
						buf := make([]byte, 1024)
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						frame, err := protocol.ParseFrame(buf[:n])
						if err != nil {
							return
						}
						if frame.Command == protocol.CmdSubscribe {
							_ = router.Subscribe(ctx, s, frame)
						}
					}(stream)
				}
			}()
		}
	}()

	conn := dialQUIC(b, ln.Addr().String(), cTLS)
	sub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		b.Fatalf("open sub: %v", err)
	}
	sendFrame(b, sub, protocol.CmdSubscribe, 1, []byte("topic:nack-bench"))
	time.Sleep(300 * time.Millisecond)

	// Pre-publish a message and get its offset
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 10,
		Payload:    []byte("nack-bench"),
	})

	// Consume the message
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	if _, err := sub.Read(buf); err != nil {
		b.Fatalf("read before bench: %v", err)
	}

	// Pre-publish messages for benchmark
	for i := 0; i < 100; i++ {
		_ = router.Publish(ctx, protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: 10,
			Payload:    []byte("nack-bench"),
		})
		// Drain
		sub.SetReadDeadline(time.Now().Add(1 * time.Second))
		if _, err := sub.Read(buf); err != nil {
			break
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		router.NackByStream(1, 1)
	}

	// Must be 0 allocs/op on the NackByStream path (no allocation for the non-blocking channel send)
}

func TestStarvationPrevention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil, WithQueueSize(20000))
	defer router.Close()

	subQueues := &[4]chan *MessageRef{}
	subMu := &sync.RWMutex{}

	router.mu.Lock()
	router.streamIDs = append(router.streamIDs, 1)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, "starvation-test")
	router.active = append(router.active, true)
	router.queues = append(router.queues, subQueues)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, subMu)
	router.cancels = append(router.cancels, func() {})
	topicHash := authz.CombineHashStrings("topic", "starvation-test")
	router.topicIndex[topicHash] = []int{0}
	router.mu.Unlock()

	extP3 := protocol.BuildPriorityExtension(protocol.PriorityLow)
	extP0 := protocol.BuildPriorityExtension(protocol.PriorityHighest)
	defer protocol.ReleaseExtensions(extP3)
	defer protocol.ReleaseExtensions(extP0)

	p3Payload := []byte("starvation-test")
	for i := 0; i < 10000; i++ {
		_ = router.Publish(ctx, protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: uint32(len(p3Payload)),
			Payload:    p3Payload,
			Extensions: extP3,
		})
	}

	p0Payload := []byte("starvation-test")
	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(p0Payload)),
		Payload:    p0Payload,
		Extensions: extP0,
	})

	msgRef, p := router.fetchNextMessage(subMu, subQueues)
	if msgRef == nil {
		t.Fatal("expected message ref")
	}
	defer msgRef.Release()

	if p != protocol.PriorityHighest {
		t.Fatalf("expected priority 0 first, got priority %d", p)
	}
}

func TestPerPriorityTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttls := [4]time.Duration{100 * time.Millisecond, 0, 0, 0}
	router := NewRouter(nil, WithPriorityTTLs(ttls))
	defer router.Close()

	subQueues := &[4]chan *MessageRef{}
	subMu := &sync.RWMutex{}

	router.mu.Lock()
	router.streamIDs = append(router.streamIDs, 1)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, "ttl-test")
	router.active = append(router.active, true)
	router.queues = append(router.queues, subQueues)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, subMu)
	router.cancels = append(router.cancels, func() {})
	topicHash := authz.CombineHashStrings("topic", "ttl-test")
	router.topicIndex[topicHash] = []int{0}
	router.mu.Unlock()

	extP0 := protocol.BuildPriorityExtension(protocol.PriorityHighest)
	defer protocol.ReleaseExtensions(extP0)

	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len("ttl-test")),
		Payload:    []byte("ttl-test"),
		Extensions: extP0,
	})

	time.Sleep(200 * time.Millisecond)

	msgRef, p := router.fetchNextMessage(subMu, subQueues)
	if msgRef == nil {
		t.Fatal("expected message ref in queue before expiration check")
	}

	nowNano := time.Now().UnixNano()
	if !msgRef.IsExpired(nowNano) {
		t.Fatal("expected message to be expired after 200ms with 100ms TTL")
	}
	msgRef.Release()
	if p != protocol.PriorityHighest {
		t.Fatalf("expected priority 0, got %d", p)
	}
}

func TestMemoryEfficiencyLazyInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil)
	defer router.Close()

	router.mu.Lock()
	for i := 0; i < 1000; i++ {
		router.streamIDs = append(router.streamIDs, uint32(i+1))
		router.streams = append(router.streams, nil)
		router.topics = append(router.topics, "lazy-test")
		router.active = append(router.active, true)
		router.queues = append(router.queues, &[4]chan *MessageRef{})
		router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
		router.subMus = append(router.subMus, &sync.RWMutex{})
		router.cancels = append(router.cancels, func() {})
		topicHash := authz.CombineHashStrings("topic", "lazy-test")
		router.topicIndex[topicHash] = append(router.topicIndex[topicHash], i)
	}
	router.mu.Unlock()

	extP2 := protocol.BuildPriorityExtension(protocol.PriorityNormal)
	defer protocol.ReleaseExtensions(extP2)

	_ = router.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len("lazy-test")),
		Payload:    []byte("lazy-test"),
		Extensions: extP2,
	})

	router.mu.RLock()
	defer router.mu.RUnlock()
	for i := 0; i < 1000; i++ {
		router.subMus[i].RLock()
		q0 := router.queues[i][0]
		q1 := router.queues[i][1]
		q2 := router.queues[i][2]
		q3 := router.queues[i][3]
		router.subMus[i].RUnlock()

		if q0 != nil || q1 != nil || q3 != nil {
			t.Fatalf("subscriber %d had non-nil priority queues (q0=%v, q1=%v, q3=%v)", i, q0, q1, q3)
		}
		if q2 == nil {
			t.Fatalf("subscriber %d priority 2 queue was not lazily initialized", i)
		}
	}
}

func BenchmarkPriorityPublish(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttls := [4]time.Duration{500 * time.Millisecond, 5 * time.Second, 0, 0}
	router := NewRouter(nil, WithPriorityTTLs(ttls))
	defer router.Close()

	subQueues := &[4]chan *MessageRef{}
	subMu := &sync.RWMutex{}

	router.mu.Lock()
	router.streamIDs = append(router.streamIDs, 1)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, "bench-prio")
	router.active = append(router.active, true)
	router.queues = append(router.queues, subQueues)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, subMu)
	router.cancels = append(router.cancels, func() {})
	topicHash := authz.CombineHashStrings("topic", "bench-prio")
	router.topicIndex[topicHash] = []int{0}
	router.mu.Unlock()

	exts := [4][]byte{
		protocol.BuildPriorityExtension(0),
		protocol.BuildPriorityExtension(1),
		protocol.BuildPriorityExtension(2),
		protocol.BuildPriorityExtension(3),
	}
	defer func() {
		for _, e := range exts {
			protocol.ReleaseExtensions(e)
		}
	}()

	payload := []byte("bench-prio")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := uint8(i % 4)
		frame := protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: uint32(len(payload)),
			Payload:    payload,
			Extensions: exts[p],
		}
		_ = router.Publish(ctx, frame)

		subMu.RLock()
		q := subQueues[p]
		subMu.RUnlock()
		if q != nil {
			select {
			case msgRef := <-q:
				if msgRef != nil {
					msgRef.Release()
				}
			default:
			}
		}
	}
}

func TestCompetingConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil)
	defer router.Close()

	topic := "orders"
	groupID := "workers"
	numWorkers := 3
	numMessages := 3000

	queues := make([]*[4]chan *MessageRef, numWorkers)
	subMus := make([]*sync.RWMutex, numWorkers)

	router.mu.Lock()
	for i := 0; i < numWorkers; i++ {
		subQueues := &[4]chan *MessageRef{}
		subMu := &sync.RWMutex{}
		queues[i] = subQueues
		subMus[i] = subMu

		router.streamIDs = append(router.streamIDs, uint32(i+1))
		router.streams = append(router.streams, nil)
		router.topics = append(router.topics, topic)
		router.active = append(router.active, true)
		router.queues = append(router.queues, subQueues)
		router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
		router.subMus = append(router.subMus, subMu)
		router.nackChs = append(router.nackChs, make(chan uint64, 8))
		router.cancels = append(router.cancels, func() {})
		router.subGroups = append(router.subGroups, groupID)

		topicHash := authz.CombineHashStrings("topic", topic)
		var cg *ConsumerGroup
		for _, g := range router.groups[topicHash] {
			if g.groupID == groupID {
				cg = g
				break
			}
		}
		if cg == nil {
			cg = &ConsumerGroup{groupID: groupID, topic: topic}
			router.groups[topicHash] = append(router.groups[topicHash], cg)
		}
		router.rebuildGroupMembersLocked(cg)
	}
	router.mu.Unlock()

	counts := make([]int, numWorkers)

	payload := []byte(topic)
	frame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(payload)),
		Payload:    payload,
	}

	for i := 0; i < numMessages; i++ {
		err := router.Publish(ctx, frame)
		if err != nil {
			t.Fatalf("publish error: %v", err)
		}
	}

	for i := 0; i < numWorkers; i++ {
		subMu := subMus[i]
		subQueues := queues[i]
		for {
			msgRef, _ := router.fetchNextMessage(subMu, subQueues)
			if msgRef == nil {
				break
			}
			counts[i]++
			msgRef.Release()
		}
	}

	totalReceived := 0
	for i, c := range counts {
		totalReceived += c
		t.Logf("Worker %d received %d messages", i, c)
		if c < 950 || c > 1050 {
			t.Errorf("Worker %d expected ~1000 messages, got %d", i, c)
		}
	}

	if totalReceived != numMessages {
		t.Fatalf("Expected total %d messages received, got %d", numMessages, totalReceived)
	}
}

func TestGroupRebalancingAndDurable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil)
	defer router.Close()

	topic := "orders"
	groupID := "workers"

	qA := &[4]chan *MessageRef{}
	muA := &sync.RWMutex{}
	qB := &[4]chan *MessageRef{}
	muB := &sync.RWMutex{}

	router.mu.Lock()
	router.streamIDs = append(router.streamIDs, 1)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, topic)
	router.active = append(router.active, true)
	router.queues = append(router.queues, qA)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, muA)
	router.nackChs = append(router.nackChs, make(chan uint64, 8))
	router.cancels = append(router.cancels, func() {})
	router.subGroups = append(router.subGroups, groupID)

	router.streamIDs = append(router.streamIDs, 2)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, topic)
	router.active = append(router.active, true)
	router.queues = append(router.queues, qB)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, muB)
	router.nackChs = append(router.nackChs, make(chan uint64, 8))
	router.cancels = append(router.cancels, func() {})
	router.subGroups = append(router.subGroups, groupID)

	topicHash := authz.CombineHashStrings("topic", topic)
	cg := &ConsumerGroup{groupID: groupID, topic: topic}
	router.groups[topicHash] = append(router.groups[topicHash], cg)
	router.rebuildGroupMembersLocked(cg)
	router.mu.Unlock()

	payload := []byte(topic)
	frame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(payload)),
		Payload:    payload,
	}

	for i := 0; i < 10; i++ {
		_ = router.Publish(ctx, frame)
	}

	router.AckOffset(groupID, topic, 10)
	if got := router.GetGroupOffset(groupID, topic); got != 10 {
		t.Fatalf("Expected group offset 10, got %d", got)
	}

	router.Unsubscribe(1)

	for i := 0; i < 5; i++ {
		_ = router.Publish(ctx, frame)
	}

	workerBMsgs := 0
	for {
		msgRef, _ := router.fetchNextMessage(muB, qB)
		if msgRef == nil {
			break
		}
		workerBMsgs++
		msgRef.Release()
	}

	if workerBMsgs != 10 {
		t.Fatalf("Worker B expected 10 total messages (5 initial + 5 failover), got %d", workerBMsgs)
	}

	router.AckOffset(groupID, topic, 15)

	qA2 := &[4]chan *MessageRef{}
	muA2 := &sync.RWMutex{}
	router.mu.Lock()
	router.streamIDs = append(router.streamIDs, 3)
	router.streams = append(router.streams, nil)
	router.topics = append(router.topics, topic)
	router.active = append(router.active, true)
	router.queues = append(router.queues, qA2)
	router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
	router.subMus = append(router.subMus, muA2)
	router.nackChs = append(router.nackChs, make(chan uint64, 8))
	router.cancels = append(router.cancels, func() {})
	router.subGroups = append(router.subGroups, groupID)
	router.rebuildGroupMembersLocked(cg)
	router.mu.Unlock()

	if got := router.GetGroupOffset(groupID, topic); got != 15 {
		t.Fatalf("Expected group offset 15 after Worker A reconnect, got %d", got)
	}
}

func BenchmarkGroupRouting(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := NewRouter(nil)
	defer router.Close()

	topic := "bench-group"
	groupID := "workers"
	numWorkers := 10

	router.mu.Lock()
	for i := 0; i < numWorkers; i++ {
		subQueues := &[4]chan *MessageRef{}
		subMu := &sync.RWMutex{}
		router.streamIDs = append(router.streamIDs, uint32(i+1))
		router.streams = append(router.streams, nil)
		router.topics = append(router.topics, topic)
		router.active = append(router.active, true)
		router.queues = append(router.queues, subQueues)
		router.notifyChs = append(router.notifyChs, make(chan struct{}, 1))
		router.subMus = append(router.subMus, subMu)
		router.nackChs = append(router.nackChs, make(chan uint64, 8))
		router.cancels = append(router.cancels, func() {})
		router.subGroups = append(router.subGroups, groupID)

		topicHash := authz.CombineHashStrings("topic", topic)
		var cg *ConsumerGroup
		for _, g := range router.groups[topicHash] {
			if g.groupID == groupID {
				cg = g
				break
			}
		}
		if cg == nil {
			cg = &ConsumerGroup{groupID: groupID, topic: topic}
			router.groups[topicHash] = append(router.groups[topicHash], cg)
		}
		router.rebuildGroupMembersLocked(cg)
	}
	router.mu.Unlock()

	payload := []byte(topic)
	frame := protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: uint32(len(payload)),
		Payload:    payload,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = router.Publish(ctx, frame)
	}
}

