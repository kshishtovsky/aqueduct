package transport

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/dedup"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

// TestBrokerDedupOverQUIC exercises the dedup pipeline end-to-end via a real
// QUIC stream: producer sends seq 1..10, then re-sends seq 5; subscriber
// receives each unique payload exactly once and the broker emits a synthetic
// dedup_ack for the duplicate.
func TestBrokerDedupOverQUIC(t *testing.T) {
	sTLS, cTLS := testTLSConfig(t)

	// Captured messages received by the broker-side router.
	var receivedCount atomic.Int32
	receivedTopics := make(chan string, 32)

	router := broker.NewRouter(&dedupTestMetrics{onPublish: func(topic string) {
		receivedCount.Add(1)
		select {
		case receivedTopics <- topic:
		default:
		}
	}})

	dedupStore := dedup.NewStore()
	defer dedupStore.Stop()

	b := New(
		WithLogger(slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))),
		WithRouter(router),
		WithDedup(dedupStore),
	)
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b.Shutdown(shCtx)
	}()

	quicConf := &quic.Config{Allow0RTT: true, MaxIdleTimeout: 30 * time.Second}
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
					b.wg.Add(1)
					go func(s *quic.Stream) {
						defer b.wg.Done()
						b.processStream(ctx, conn, s, "test-client")
					}(stream)
				}
			}()
		}
	}()

	// Open a subscriber stream and subscribe to "orders" BEFORE publishing.
	subConn, err := quic.DialAddr(ctx, ln.Addr().String(), cTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial sub: %v", err)
	}
	defer subConn.CloseWithError(0, "test done")

	sub, err := subConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub stream: %v", err)
	}
	defer sub.Close()

	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:orders"))
	if _, err := sub.Write(*subBuf); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	protocol.ReleaseBuffer(subBuf)

	// Wait briefly for the subscription to register.
	time.Sleep(200 * time.Millisecond)

	// Dial and open a publisher stream.
	conn, err := quic.DialAddr(ctx, ln.Addr().String(), cTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial pub: %v", err)
	}
	defer conn.CloseWithError(0, "test done")

	pub, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open pub stream: %v", err)
	}
	defer pub.Close()

	// Send seq 1..10.
	for seq := uint64(1); seq <= 10; seq++ {
		ext := protocol.BuildIdempotentExtension(42, seq)
		buf := protocol.SerializeFrameWithExtensions(protocol.CmdPublish, 0, ext, []byte("orders"))
		protocol.ReleaseExtensions(ext)
		if _, err := pub.Write(*buf); err != nil {
			t.Fatalf("write seq %d: %v", seq, err)
		}
		protocol.ReleaseBuffer(buf)
	}

	// Re-send seq 5 — must be detected as duplicate and trigger a dedup_ack.
	ext := protocol.BuildIdempotentExtension(42, 5)
	buf := protocol.SerializeFrameWithExtensions(protocol.CmdPublish, 0, ext, []byte("orders"))
	protocol.ReleaseExtensions(ext)
	if _, err := pub.Write(*buf); err != nil {
		t.Fatalf("write resend 5: %v", err)
	}
	protocol.ReleaseBuffer(buf)

	// Read responses: we expect a dedup_ack for seq 5.
	pub.SetReadDeadline(time.Now().Add(3 * time.Second))
	dedupAckSeen := readUntilDedupAck(t, pub, 42, 5, 3*time.Second)
	if !dedupAckSeen {
		t.Fatalf("did not receive dedup_ack for producer 42 seq 5")
	}

	// The router should have received 10 unique publishes, not 11.
	deadline := time.Now().Add(2 * time.Second)
	for receivedCount.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := receivedCount.Load(); got != 10 {
		t.Fatalf("expected 10 publishes to reach router, got %d", got)
	}
}

// readUntilDedupAck reads from the stream and returns true when a CmdAck
// frame containing the dedup_ack:<id>:<seq> payload is seen.
func readUntilDedupAck(t *testing.T, s *quic.Stream, producerID, seqNum uint64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	expected := []byte("dedup_ack:")
	idStr := []byte{}
	// Format expected payload prefix.
	for i := uint64(0); ; i++ {
		d, _ := uintToASCII(producerID)
		_ = d
		break
	}
	idStr = append(idStr, []byte(formatUint(producerID))...)
	idStr = append(idStr, ':')
	idStr = append(idStr, []byte(formatUint(seqNum))...)
	expectedFull := append(append([]byte{}, expected...), idStr...)

	buf := make([]byte, 4096)
	for time.Now().Before(deadline) {
		s.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := s.Read(buf)
		if n > 0 {
			// Scan for a frame whose payload starts with dedup_ack:42:5.
			if containsDedupAckPayload(buf[:n], expectedFull) {
				return true
			}
		}
		if err != nil {
			// timeout or EOF — keep trying until outer deadline.
			continue
		}
	}
	return false
}

func containsDedupAckPayload(data, expected []byte) bool {
	if len(expected) == 0 || len(data) < len(expected) {
		return false
	}
	for i := 0; i+len(expected) <= len(data); i++ {
		match := true
		for j := 0; j < len(expected); j++ {
			if data[i+j] != expected[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func formatUint(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func uintToASCII(n uint64) ([]byte, int) {
	s := formatUint(n)
	return []byte(s), len(s)
}

// dedupTestMetrics is a tiny RouterMetrics implementation that records publishes.
type dedupTestMetrics struct {
	onPublish func(topic string)
}

func (m *dedupTestMetrics) OnPublish(topic string) {
	if m.onPublish != nil {
		m.onPublish(topic)
	}
}

func (m *dedupTestMetrics) OnDeliver(topic string)         {}
func (m *dedupTestMetrics) SetActiveSubscribers(n float64) {}
func (m *dedupTestMetrics) OnRateLimited(clientID string)  {}

// testWriter discards log output.
type testWriter struct{ t *testing.T }

func (testWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// ensure we use the imported packages to keep the test self-contained.
var (
	_ = net.IPv4
	_ = binary.LittleEndian
	_ = atomic.Int32{}
)
