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
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
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
	published atomic.Int64
	delivered atomic.Int64
	subs      atomic.Int64
}

func (m *mockMetrics) OnPublish(topic string) { m.published.Add(1) }
func (m *mockMetrics) OnDeliver(topic string) { m.delivered.Add(1) }
func (m *mockMetrics) SetActiveSubscribers(n float64) {
	m.subs.Store(int64(n))
}

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
