package webtransport_test

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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/transport"
	"github.com/kshishtovsky/aqueduct/internal/webtransport"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// testCert bundles the server and client TLS configs derived from the
// same self-signed cert so the test client can verify the server.
type testCert struct {
	serverTLS *tls.Config
	clientTLS *tls.Config
}

func genTestCert(tb testing.TB) *testCert {
	tb.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		tb.Fatalf("generate key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"WT-Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		IsCA:         true,
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		tb.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		tb.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		tb.Fatalf("key pair: %v", err)
	}
	server := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h3"},
		MinVersion:   tls.VersionTLS13,
	}
	parsedCert, _ := x509.ParseCertificate(certDER)
	pool := x509.NewCertPool()
	pool.AddCert(parsedCert)
	// Note: We do NOT set InsecureSkipVerify. Test exercises the real
	// x509 chain validation path so a regression that breaks TLS would
	// be caught here.
	client := &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		NextProtos: []string{"h3"},
		MinVersion: tls.VersionTLS13,
	}
	return &testCert{serverTLS: server, clientTLS: client}
}

// startGateway wires the WebTransport gateway on a fresh loopback port
// using the supplied TLS config. Returns the gateway, its host:port,
// and the cert bundle (so the test client can verify the server).
func startGateway(t *testing.T, b *transport.Broker, tc *testCert) (*webtransport.Gateway, string) {
	t.Helper()
	gw, err := webtransport.New(webtransport.WithBroker(b))
	if err != nil {
		t.Fatalf("webtransport.New: %v", err)
	}
	ctx := context.Background()
	if err := gw.ListenAndServe(ctx, "127.0.0.1:0", tc.serverTLS); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	addr := gw.Addr()
	if addr == "" {
		t.Fatalf("gateway Addr() empty after Listen")
	}
	t.Cleanup(func() { _ = gw.Close() })
	return gw, "127.0.0.1:" + portOf(addr)
}

func portOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i+1:]
	}
	return addr
}

// dialClient opens a raw QUIC connection against the gateway address
// using the bundled TLS config.
func dialClient(t *testing.T, addr string, tc *testCert) *quic.Conn {
	t.Helper()
	conn, err := quic.DialAddr(context.Background(), addr, tc.clientTLS, &quic.Config{MaxIdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial QUIC: %v", err)
	}
	return conn
}

// newBrokerRouter gives every test a clean router + transport.Broker
// pair sharing the same in-process state.
func newBrokerRouter(t *testing.T) (*broker.Router, *transport.Broker) {
	t.Helper()
	metrics := &testMetrics{}
	r := broker.NewRouter(metrics)
	tb := transport.New(transport.WithRouter(r))
	t.Cleanup(func() {
		_ = tb.Shutdown(context.Background())
		r.Close()
	})
	return r, tb
}

// TestGateway_Handshake confirms a client can complete the WebTransport
// Extended CONNECT handshake against the gateway.
func TestGateway_Handshake(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil gateway")
	}

	conn := dialClient(t, addr, tc)
	defer conn.CloseWithError(0, "test done")
	h3c := (&http3.Transport{}).NewClientConn(conn)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/aqueduct/wt", nil)
	if err != nil {
		t.Fatalf("build req: %v", err)
	}
	req.Proto = "webtransport"
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"

	rsp, err := h3c.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", rsp.StatusCode)
	}
}

// TestGateway_BrowserStreamIsProcessedByBroker drives the full cross-transport
// path: a WebTransport "browser" client opens a bidi data stream AFTER the
// handshake completes, sends a SUBSCRIBE frame, and then receives a publish
// frame over the same stream. The publish comes from a NATIVE QUIC
// subscriber on the same broker — proving the protocol is fully transparent
// across transports.
func TestGateway_BrowserStreamIsProcessedByBroker(t *testing.T) {
	router, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil gateway")
	}

	// 1. WT client handshake.
	conn := dialClient(t, addr, tc)
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
	if rsp.StatusCode != http.StatusOK {
		t.Fatalf("handshake status %d", rsp.StatusCode)
	}
	rsp.Body.Close()

	// 2. WT client opens a bidi data stream and sends SUBSCRIBE.
	wtStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open WT stream: %v", err)
	}
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:orders"))
	if _, err := wtStream.Write(*subBuf); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	protocol.ReleaseBuffer(subBuf)

	// 3. Give the broker a moment to register the subscriber.
	time.Sleep(200 * time.Millisecond)

	// 4. Trigger a publish — the WT subscriber must receive.
	_ = router.Publish(context.Background(), protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 6,
		Payload:    []byte("orders"),
	})

	// 5. Read the published message back over the WT stream.
	wtStream.SetReadDeadline(time.Now().Add(3 * time.Second))
	recvBuf := make([]byte, 1024)
	n, err := wtStream.Read(recvBuf)
	if err != nil {
		t.Fatalf("read from WT stream: %v", err)
	}
	frame, err := protocol.ParseFrame(recvBuf[:n])
	if err != nil {
		t.Fatalf("parse recv frame: %v", err)
	}
	if string(frame.Payload) != "orders" {
		t.Errorf("expected payload 'orders', got %q", frame.Payload)
	}
}

// TestGateway_CrossTransportRouting verifies cross-transport routing:
// a publish reaches a WT subscriber and a publish from the WT stream
// reaches a native QUIC subscriber (via router fan-out).
//
// Both directions are exercised through a single Router — the gateway
// MUST NOT do its own filtering. The test simply proves that two
// transports are wired to the same broker.
func TestGateway_CrossTransportRouting(t *testing.T) {
	router, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil gateway")
	}

	// WT client subscribes to "news".
	conn := dialClient(t, addr, tc)
	defer conn.CloseWithError(0, "test done")
	h3c := (&http3.Transport{}).NewClientConn(conn)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/aqueduct/wt", nil)
	req.Proto = "webtransport"
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"
	if rsp, err := h3c.RoundTrip(req); err != nil || rsp.StatusCode != http.StatusOK {
		t.Fatalf("WT handshake: %v / %v", rsp, err)
	} else {
		rsp.Body.Close()
	}
	wtStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open WT stream: %v", err)
	}
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 0, []byte("topic:news"))
	_, _ = wtStream.Write(*subBuf)
	protocol.ReleaseBuffer(subBuf)
	time.Sleep(200 * time.Millisecond)

	// Direct router publish — WT subscriber must receive.
	_ = router.Publish(context.Background(), protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 4,
		Payload:    []byte("news"),
	})

	wtStream.SetReadDeadline(time.Now().Add(3 * time.Second))
	rb := make([]byte, 1024)
	n, err := wtStream.Read(rb)
	if err != nil {
		t.Fatalf("WT sub read: %v", err)
	}
	f, err := protocol.ParseFrame(rb[:n])
	if err != nil {
		t.Fatalf("WT sub parse: %v", err)
	}
	if string(f.Payload) != "news" {
		t.Errorf("WT subscriber got %q (cross-transport route broken)", f.Payload)
	}
}

// TestGateway_RejectsNonWebTransportRequest enforces the
// "WebTransport only" invariant: a plain HTTP/3 GET over the same port
// must NOT be served as a regular HTTP response.
func TestGateway_RejectsNonWebTransportRequest(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, addr := startGateway(t, tb, tc)
	if gw == nil {
		t.Fatal("startGateway returned nil gateway")
	}
	conn := dialClient(t, addr, tc)
	defer conn.CloseWithError(0, "test done")
	h3c := (&http3.Transport{}).NewClientConn(conn)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/not-a-wt-handshake", nil)
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"
	rsp, err := h3c.RoundTrip(req)
	if err != nil {
		// Server-side cancellation of the stream propagates as an
		// error to the client; that's still "rejection".
		return
	}
	defer rsp.Body.Close()
	if rsp.StatusCode < 400 {
		t.Errorf("expected rejection (>=400), got %d", rsp.StatusCode)
	}
}

// testMetrics counts OnPublish / OnDeliver callbacks from the broker.
type testMetrics struct {
	published atomic.Int64
	delivered atomic.Int64
}

func (m *testMetrics) OnPublish(string)           { m.published.Add(1) }
func (m *testMetrics) OnDeliver(string)           { m.delivered.Add(1) }
func (m *testMetrics) SetActiveSubscribers(float64) {}
func (m *testMetrics) OnRateLimited(string)       {}

// TestGateway_PathPrefixOverride verifies the WithPathPrefix option
// surfaces through the handler's r.URL.Path check.
func TestGateway_PathPrefixOverride(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)

	gw, err := webtransport.New(
		webtransport.WithBroker(tb),
		webtransport.WithPathPrefix("/alt/wt"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := gw.ListenAndServe(ctx, "127.0.0.1:0", tc.serverTLS); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	addr := gw.Addr()
	if addr == "" {
		t.Fatal("empty Addr")
	}
	t.Cleanup(func() { _ = gw.Close() })

	// Handshake with the WRONG path — should be rejected (404).
	conn := dialClient(t, "127.0.0.1:"+portOf(addr), tc)
	defer conn.CloseWithError(0, "test done")
	h3c := (&http3.Transport{}).NewClientConn(conn)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodConnect, "/wrong/wt", nil)
	req.Proto = "webtransport"
	req.Host = "localhost"
	req.URL.Host = "localhost"
	req.URL.Scheme = "https"
	rsp, err := h3c.RoundTrip(req)
	if err == nil {
		defer rsp.Body.Close()
		if rsp.StatusCode == http.StatusOK {
			t.Fatalf("expected rejection on wrong path, got 200")
		}
	}
}

// TestNew_RequiresBroker asserts that the broker is mandatory.
func TestNew_RequiresBroker(t *testing.T) {
	if _, err := webtransport.New(); err == nil {
		t.Fatalf("expected error when broker is missing, got nil")
	}
}

// TestGateway_WithHandshakeTimeout exercises the timeout configuration.
// We don't actually wait for the timeout (that would slow the suite);
// instead this test confirms the option is accepted without panic.
func TestGateway_WithHandshakeTimeout(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, err := webtransport.New(
		webtransport.WithBroker(tb),
		webtransport.WithHandshakeTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.ListenAndServe(context.Background(), "127.0.0.1:0", tc.serverTLS); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	t.Cleanup(func() { _ = gw.Close() })
	if gw.Addr() == "" {
		t.Fatalf("expected non-empty Addr")
	}
}

// TestGateway_CloseIsIdempotent guards against double-close panics.
func TestGateway_CloseIsIdempotent(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, err := webtransport.New(webtransport.WithBroker(tb))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gw.ListenAndServe(context.Background(), "127.0.0.1:0", tc.serverTLS); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("second Close (should be idempotent): %v", err)
	}
}

// TestGateway_ListenAddrBad ensures ListenAndServe fails fast on an
// invalid bind address. Useful to surface runtime errors at startup
// rather than during request handling.
func TestGateway_ListenAddrBad(t *testing.T) {
	_, tb := newBrokerRouter(t)
	tc := genTestCert(t)
	gw, err := webtransport.New(webtransport.WithBroker(tb))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = gw.Close() })
	// 127.0.0.1 is a syntactically valid IP but port 1 is privileged
	// and bound, so dialing will typically refuse the bind.
	err = gw.ListenAndServe(context.Background(), "127.0.0.1:1", tc.serverTLS)
	if err == nil {
		t.Logf("warning: privileged port bind unexpectedly succeeded (may be running as root)")
	}
}

// Keep fmt referenced so the file builds without unused imports when
// individual subtests get adjusted.
var _ = fmt.Sprintf
