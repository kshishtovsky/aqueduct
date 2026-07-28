package webtransport_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/transport"
	"github.com/kshishtovsky/aqueduct/internal/webtransport"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// BenchmarkWTGateway_Handshake measures how long the gateway takes to
// complete one Extended CONNECT handshake. Useful for capacity planning
// of concurrent handshakes per second.
func BenchmarkWTGateway_Handshake(b *testing.B) {
	routerMetrics := &testMetrics{}
	router := broker.NewRouter(routerMetrics)
	tb := transport.New(transport.WithRouter(router))
	tc := genTestCert(b)
	gw, err := webtransport.New(webtransport.WithBroker(tb))
	if err != nil {
		b.Fatalf("gateway: %v", err)
	}
	if err := gw.ListenAndServe(context.Background(), "127.0.0.1:0", tc.serverTLS); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() {
		_ = gw.Close()
		_ = tb.Shutdown(context.Background())
		router.Close()
	}()
	addr := "127.0.0.1:" + portOf(gw.Addr())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := quic.DialAddr(context.Background(), addr, tc.clientTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		h3c := (&http3.Transport{}).NewClientConn(conn)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/aqueduct/wt", nil)
		req.Proto = "webtransport"
		req.Host = "localhost"
		req.URL.Host = "localhost"
		req.URL.Scheme = "https"
		rsp, err := h3c.RoundTrip(req)
		if err != nil {
			conn.CloseWithError(0, "x")
			b.Fatalf("roundtrip: %v", err)
		}
		rsp.Body.Close()
		conn.CloseWithError(0, "x")
	}
}

// BenchmarkWTGateway_PublishLatency measures end-to-end latency from a
// router.Publish call until the WT subscriber reads the frame. Useful
// for capacity planning the per-message overhead introduced by the
// gateway in addition to the broker's own fan-out path.
func BenchmarkWTGateway_PublishLatency(b *testing.B) {
	routerMetrics := &testMetrics{}
	router := broker.NewRouter(routerMetrics)
	tb := transport.New(transport.WithRouter(router))
	tc := genTestCert(b)
	gw, err := webtransport.New(webtransport.WithBroker(tb))
	if err != nil {
		b.Fatalf("gateway: %v", err)
	}
	if err := gw.ListenAndServe(context.Background(), "127.0.0.1:0", tc.serverTLS); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() {
		_ = gw.Close()
		_ = tb.Shutdown(context.Background())
		router.Close()
	}()
	addr := "127.0.0.1:" + portOf(gw.Addr())

	// Pre-warm subscriber so the broker has time to register the
	// subscriber slot before the benchmark loop starts.
	subConn, _ := quic.DialAddr(context.Background(), addr, tc.clientTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
	defer subConn.CloseWithError(0, "x")
	h3c := (&http3.Transport{}).NewClientConn(subConn)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/aqueduct/wt", nil)
	req.Proto = "webtransport"
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"
	rsp, _ := h3c.RoundTrip(req)
	rsp.Body.Close()
	subStream, _ := subConn.OpenStreamSync(context.Background())
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:bench"))
	if _, err := subStream.Write(*subBuf); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	protocol.ReleaseBuffer(subBuf)
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = router.Publish(context.Background(), protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: 4,
			Payload:    []byte("bench"),
		})
		subStream.SetReadDeadline(time.Now().Add(1 * time.Second))
		buf := make([]byte, 1024)
		if _, err := subStream.Read(buf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
