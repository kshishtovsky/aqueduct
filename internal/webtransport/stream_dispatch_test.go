package webtransport_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/webtransport"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// TestGateway_ConcurrentSessions stresses the gateway with multiple
// concurrent WebTransport sessions; each session opens two bidi streams,
// subscribes, and reads publishes. This is the closest we can get to a
// real browser workload in a test environment, and catches races in the
// per-connection stream dispatcher.
//
// Each stream goroutine uses its own context (canceled by the parent
// when the test wants to stop them) — NOT a Read deadline — so the
// goroutines still exist at the moment the parent publishes.
func TestGateway_ConcurrentSessions(t *testing.T) {
	const numSessions = 4
	const streamsPerSession = 2

	router, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil")
	}

	var (
		connectedCount atomic.Int64
		messagesRecv   atomic.Int64
		failures       atomic.Int64
	)
	// Parent context cancels every stream goroutine when we're done.
	parentCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

	var wg sync.WaitGroup
	for i := 0; i < numSessions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := quic.DialAddr(
				parentCtx,
				addr,
				tc.clientTLS,
				&quic.Config{MaxIdleTimeout: 10 * time.Second},
			)
			if err != nil {
				failures.Add(1)
				t.Logf("session %d dial err: %v", idx, err)
				return
			}
			// Hold the connection alive for the lifetime of this
			// session's streams. We Close only after the parent
			// cancels parentCtx AND every stream goroutine has
			// returned, so streams never see a half-closed conn.
			defer conn.CloseWithError(0, "test done")

			h3c := (&http3.Transport{}).NewClientConn(conn)
			req, _ := http.NewRequestWithContext(
				parentCtx,
				http.MethodConnect,
				"/aqueduct/wt",
				nil,
			)
			req.Proto = "webtransport"
			req.Host = "localhost"
			req.URL.Host = "localhost"
			req.URL.Scheme = "https"

			rsp, err := h3c.RoundTrip(req)
			if err != nil {
				failures.Add(1)
				t.Logf("session %d handshake err: %v", idx, err)
				return
			}
			rsp.Body.Close()
			if rsp.StatusCode != http.StatusOK {
				failures.Add(1)
				t.Logf("session %d status %d", idx, rsp.StatusCode)
				return
			}
			connectedCount.Add(1)
			t.Logf("session %d handshake OK", idx)

			// Spawn bidi streams as children of this session so
			// wg.Done on the parent session happens AFTER the
			// streams have all exited — NOT before, otherwise the
			// session-level `defer conn.CloseWithError` would fire
			// mid-publish.
			var streamWG sync.WaitGroup
			for s := 0; s < streamsPerSession; s++ {
				streamWG.Add(1)
				go func(streamIdx int) {
					defer streamWG.Done()
					stream, err := conn.OpenStreamSync(parentCtx)
					if err != nil {
						failures.Add(1)
						t.Logf("session %d stream %d OpenStreamSync err: %v", idx, streamIdx, err)
						return
					}
					topic := "orders"
					subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:"+topic))
					if _, err := stream.Write(*subBuf); err != nil {
						protocol.ReleaseBuffer(subBuf)
						failures.Add(1)
						return
					}
					protocol.ReleaseBuffer(subBuf)

					// Wait until parent signals publish is done, OR a
					// generous timeout. Use parentCtx so the parent can
					// cancel us; do NOT rely on a Read deadline since
					// by the time the deadline fires the goroutine is
					// already too late to receive the publish.
					scratch := make([]byte, 4096)
					off := 0
				drain:
					for {
						select {
						case <-parentCtx.Done():
							return
						default:
						}
						if off == cap(scratch) {
							t.Logf("session %d stream %d scratch overflow", idx, streamIdx)
							failures.Add(1)
							return
						}
						stream.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
						n, err := stream.Read(scratch[off:cap(scratch)])
						if err != nil {
							if parentCtx.Err() != nil {
								return
							}
							// Periodic deadline — try again. Do not
							// increment failures on transient read
							// timeouts (that's the polling mechanism).
							continue
						}
						off += n
						if off < protocol.HeaderSize {
							continue
						}
						view := scratch[:off]
						_, _, err = protocol.ParseBatchFrame(view, 0)
						if err != nil {
							continue
						}
						frame, perr := protocol.ParseFrame(view)
						if perr != nil {
							t.Errorf("session %d stream %d parse: %v", idx, streamIdx, perr)
							return
						}
						if string(frame.Payload) != topic {
							t.Errorf("session %d stream %d got %q (expected %q)", idx, streamIdx, frame.Payload, topic)
							return
						}
						messagesRecv.Add(1)
						break drain
					}
				}(s)
			}
			// Block until every stream from this session has
			// returned. The session-level defer on `conn.CloseWithError`
			// must NOT fire while streams are still trying to
			// OpenStreamSync.
			streamWG.Wait()
		}(i)
	}

	// Wait briefly so subscribers are actually registered across
	// every concurrent session. 500 ms is conservative — the broker's
	// read goroutine for each stream registers synchronously.
	time.Sleep(500 * time.Millisecond)

	// Trigger ONE publish; EVERY subscriber (across all sessions) must
	// receive it.
	if err := router.Publish(context.Background(), protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 6,
		Payload:    []byte("orders"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Give the broker up to 5 s to fan-out.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if messagesRecv.Load() >= int64(numSessions*streamsPerSession) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelAll()
	wg.Wait()

	if connectedCount.Load() != numSessions {
		t.Errorf("expected %d connected sessions, got %d", numSessions, connectedCount.Load())
	}
	expected := int64(numSessions * streamsPerSession)
	if messagesRecv.Load() != expected {
		t.Errorf("expected %d messages received across %d sessions, got %d", expected, numSessions, messagesRecv.Load())
	}
	if failures.Load() > 0 {
		t.Errorf("encountered %d failures during concurrent session test", failures.Load())
	}
}

// TestGateway_StreamSurvivesServerPush verifies that the broker can
// independently push data to a WT bidi stream opened by the client.
// The native-QUIC side uses Router.Publish; the WebTransport side only
// owns the read half of the stream. This mirrors the WebTransport
// session semantics where the server can write back on a stream the
// client opened.
//
// The broker uses coalesced writes (64 KB batch size), so multiple
// publishes often arrive in ONE Read at the client. The test mirrors
// the broker's own runStreamReadLoop: accumulate bytes in a buffer,
// parse frames repeatedly, only refill via Read when the buffer drains.
func TestGateway_StreamSurvivesServerPush(t *testing.T) {
	router, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil")
	}
	_ = gw

	conn, err := quic.DialAddr(context.Background(), addr, tc.clientTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "test done")
	h3c := (&http3.Transport{}).NewClientConn(conn)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/aqueduct/wt", nil)
	req.Proto = "webtransport"
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"
	rsp, err := h3c.RoundTrip(req)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	rsp.Body.Close()

	// Subscribe through the WT bidi stream.
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:alerts"))
	_, _ = stream.Write(*subBuf)
	protocol.ReleaseBuffer(subBuf)

	time.Sleep(200 * time.Millisecond)

	// Server pushes THREE messages back-to-back.
	for i := 0; i < 3; i++ {
		_ = router.Publish(context.Background(), protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: 6,
			Payload:    []byte("alerts"),
		})
	}

	// Drain the stream into a 64KB scratch buffer and parse every
	// complete frame, exactly as the broker does on the server side.
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	scratch := make([]byte, 64*1024)
	off := 0
	got := 0
	for got < 3 {
		if off == cap(scratch) {
			t.Fatalf("scratch overflow before parsing 3 frames")
		}
		n, err := stream.Read(scratch[off:cap(scratch)])
		if err != nil {
			t.Fatalf("read at off=%d (got=%d): %v", off, got, err)
		}
		// try to parse as many complete frames as scratch holds.
		local := scratch[:off+n]
		consumed := 0
		for consumed < len(local) {
			frame, err := protocol.ParseFrame(local[consumed:])
			if err != nil {
				t.Fatalf("parse at off=%d (got=%d): %v", consumed, got, err)
			}
			// ParseFrame returns nothing if the buffer is short — that
			// is, we don't have a complete header+payload yet.
			if frame.Payload == nil && frame.PayloadLen == 0 && len(local)-consumed < protocol.HeaderSize {
				break
			}
			consumed += protocol.HeaderSize + int(frame.PayloadLen)
			// Wire payload sent to subscribers is the clean topic name
			// (parsePublishTopic strips any "topic:" prefix). On the
			// wire this publish carries just the topic.
			if string(frame.Payload) != "alerts" {
				t.Errorf("msg %d: payload %q (expected alerts)", got, frame.Payload)
			}
			got++
		}
		// Slide unparsed bytes to the front of scratch.
		remaining := local[consumed:]
		copy(scratch, remaining)
		off = len(remaining)
	}
	if got != 3 {
		t.Errorf("expected 3 messages, got %d", got)
	}
}

// Compile-time assertions that options are not accidentally renamed.
var _ = webtransport.WithBroker(nil)
var _ = webtransport.WithPathPrefix
var _ = webtransport.WithHandshakeTimeout
