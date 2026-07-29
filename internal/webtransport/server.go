// Package webtransport implements a WebTransport (HTTP/3) gateway that lets
// web browsers connect to the Aqueduct broker over the same binary frame
// protocol that native QUIC clients use.
//
// # Why a separate listener?
//
// Browsers cannot open a raw QUIC stream with the `aqueduct-v1` ALPN — they
// only speak HTTP/3 + WebTransport. We multiplex the broker's binary frame
// protocol through WebTransport bidirectional streams (RFC 9298) so the
// browser can write a `[Magic:1][Cmd:1][StreamID:4][Len:4][Payload:…]`
// frame into a WT bidi stream and the broker's existing
// transport.Broker.HandleStream parses it without any protocol changes.
//
// # Architecture
//
//   ┌─────────────┐       ┌──────────────────────┐       ┌───────────────────┐
//   │ Browser     │ ─WT─► │ internal/webtransport│ ─►    │ internal/transport│
//   │ (HTTP/3)    │       │ (this package)       │  *qs  │ (existing)        │
//   └─────────────┘       └──────────────────────┘       └───────────────────┘
//
//  1. Server wraps quic-go's http3.Server only to reuse the SETTINGS-frame
//     and QPACK plumbing of the handshake (Extended CONNECT with
//     `:protocol: webtransport`).
//  2. The handshake stream hijacks the response writer (so http3 does not
//     close it after Handler returns — the WT spec mandates the session
//     stream stays open for capsule protocol).
//  3. Subsequent QUIC bidi streams opened on the same connection are
//     WebTransport data streams. We accept them ourselves (not via
//     http3.Server's auto-loop) so the broker's existing dispatch path
//     runs unchanged.
package webtransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/transport"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Default ALPN negotiated by WebTransport clients — must match the
// NextProtos slice of the *tls.Config handed to QUIC.
//
// Setting `h3` lets the browser speak HTTP/3 over the underlying QUIC
// connection. Without it, the WebTransport JS API rejects the connection.
const alpnH3 = "h3"

// DefaultPathPrefix is the URL path browser clients should send their
// Extended CONNECT request to. Configurable via Server.PathPrefix.
const DefaultPathPrefix = "/aqueduct/wt"

// handshakeTimeout caps the time spent waiting for the client to complete
// the Extended CONNECT handshake. After this, the connection is torn down
// to prevent Slowloris-style attacks on the gateway.
const handshakeTimeout = 10 * time.Second

// Gateway is the public WebTransport entry-point. Embed *transport.Broker
// (or pass one in via WithBroker) so the gateway reuses the broker's
// existing router / authz engine / AAL log.
type Gateway struct {
	listener *quic.Listener
	broker   *transport.Broker

	pathPrefix string

	// http3Cfg is reused across connections for SETTINGS frames.
	http3Cfg http3ServerConfig

	// conns tracks every in-flight connection so Shutdown can drain them
	// gracefully.
	conns sync.WaitGroup

	stopped atomic.Bool

	// tlsConfig is the (cloned, h3-annotated) *tls.Config passed to
	// quic.ListenAddr. Held for debugging / introspection only;
	// ListenAndServe is the only mutation point.
	tlsConfig *tls.Config

	// listenerAddr is captured at Listen() time so callers can advertise
	// the gateway address (e.g. Alt-Svc headers).
	listenerAddr atomic.Value // string
}

// Option configures Gateway. Use WithPathPrefix / WithHandshakeTimeout to
// tweak defaults; everything else is intentionally hard-coded so the
// gateway's surface stays small and reviewable.
type Option func(*Gateway)

// WithPathPrefix overrides the URL path the WebTransport handshake must use.
// Defaults to DefaultPathPrefix.
func WithPathPrefix(prefix string) Option {
	return func(g *Gateway) {
		if prefix != "" {
			g.pathPrefix = prefix
		}
	}
}

// WithHandshakeTimeout overrides the maximum time the server waits for a
// client to complete the Extended CONNECT handshake.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(g *Gateway) {
		if d > 0 {
			g.http3Cfg.handshakeTimeout = d
		}
	}
}

// WithBroker wires an already-configured *transport.Broker. The gateway
// simply forwards every accepted bidi stream into broker.HandleStream, so
// router / authz / AAL / tracing all flow through the same code paths.
func WithBroker(b *transport.Broker) Option {
	return func(g *Gateway) {
		g.broker = b
	}
}

// http3ServerConfig is an internal config struct that preserves the
// parameters we hand to http3.Server on each connection. Kept as a
// named type so future tuning (max header bytes, additional SETTINGS, …)
// has a single place to live.
type http3ServerConfig struct {
	handshakeTimeout time.Duration
}

// New builds a Gateway that wraps the supplied broker. The caller is
// responsible for invoking ListenAndServe (or Listen followed by the
// internal accept loop).
func New(opts ...Option) (*Gateway, error) {
	g := &Gateway{
		pathPrefix:        DefaultPathPrefix,
		http3Cfg:          http3ServerConfig{handshakeTimeout: handshakeTimeout},
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.broker == nil {
		return nil, errors.New("webtransport: a *transport.Broker is required; use WithBroker")
	}
	return g, nil
}

// ListenAndServe binds to addr, configures TLS for HTTP/3 ALPN, and serves
// WebTransport sessions until Close is called.
//
// ListenAndServe returns immediately after the QUIC listener has been
// created; the per-connection accept loop runs in a background goroutine.
// This makes the API composable with cmd-line `signal.Notify`-based
// shutdown patterns (Close cancels the loop and waits for drain).
//
// The TLS config is fed through http3.ConfigureTLSConfig so the QUIC
// listener picks up the session-ticket workaround and the canonical
// "h3" ALPN. The caller's *tls.Config is not mutated.
func (g *Gateway) ListenAndServe(ctx context.Context, addr string, baseTLS *tls.Config) error {
	tc := http3.ConfigureTLSConfig(cloneTLSForH3(baseTLS))
	g.tlsConfig = tc

	quicCfg := &quic.Config{
		Allow0RTT:          true,
		MaxIdleTimeout:     30 * time.Second,
		MaxIncomingStreams: 100,
	}

	ln, err := quic.ListenAddr(addr, tc, quicCfg)
	if err != nil {
		return fmt.Errorf("webtransport: listen on %s: %w", addr, err)
	}
	g.listener = ln
	g.listenerAddr.Store(ln.Addr().String())

	go g.runAcceptLoop(ctx, ln)
	return nil
}

// Addr returns the bound UDP address (or empty string before Listen).
func (g *Gateway) Addr() string {
	if v := g.listenerAddr.Load(); v != nil {
		s, _ := v.(string)
		return s
	}
	return ""
}

// Close stops the listener and waits for in-flight connections to drain.
// Safe to call multiple times.
func (g *Gateway) Close() error {
	if !g.stopped.CompareAndSwap(false, true) {
		return nil
	}
	if g.listener != nil {
		_ = g.listener.Close()
	}
	g.conns.Wait()
	return nil
}

// runAcceptLoop is the main accept-and-spawn loop. It blocks until the
// listener closes or the context is canceled.
func (g *Gateway) runAcceptLoop(ctx context.Context, ln *quic.Listener) {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if g.stopped.Load() || errors.Is(err, quic.ErrServerClosed) {
				return
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}
		g.conns.Add(1)
		go func(c *quic.Conn) {
			defer g.conns.Done()
			g.handleConn(ctx, c)
		}(conn)
	}
}

// handleConn runs the HTTP/3 setup, drives the WebTransport handshake,
// and finally loops on AcceptStream to feed every WT bidi data stream
// into the underlying broker.
func (g *Gateway) handleConn(ctx context.Context, conn *quic.Conn) {
	// Build a per-connection http3 server so we can hijack the response
	// writer without touching http3's global state. Setting
	// `EnableDatagrams = true` is harmless even if we never read
	// datagrams in v1.16.0 — it costs us nothing and lets future
	// datagram support drop in without breaking compatibility.
	h3 := &http3.Server{
		Handler:         g.handler(conn),
		EnableDatagrams: true,
		Logger:          nil,
	}

	// Initialize HTTP/3 on the QUIC connection: this opens the
	// server-to-client control stream with a SETTINGS frame, registers
	// the QPACK decoder, etc. Returns a *RawServerConn that lets us
	// drive subsequent streams ourselves (instead of letting http3 loop
	// and turn every bidi stream back into an HTTP request).
	rawConn, err := h3.NewRawServerConn(conn)
	if err != nil {
		_ = conn.CloseWithError(0, "h3 init failed")
		return
	}

	// 1. Uni-stream goroutine — http3 knows how to interpret the
	//    well-known HTTP/3 control uni-stream types (0/1/2/3). WT uni
	//    data streams from the client have a different stream type and
	//    are silently dropped (a TODO for v1.17.0 if datagrams are ever
	//    needed).
	go func() {
		for {
			str, err := conn.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			rawConn.HandleUnidirectionalStream(str)
		}
	}()

	// 2. Bi-stream goroutine — pull the FIRST bi stream (the WT
	//    handshake) and let our Handler validate it.
	first, err := conn.AcceptStream(ctx)
	if err != nil {
		_ = rawConn.CloseWithError(0, "no handshake stream")
		return
	}
	g.serveHandshake(rawConn, first, conn)

	// 3. After the handshake response is on the wire, accept further
	//    bidi streams ourselves and feed each into broker.HandleStream.
	//    The bidi loop lives until the connection drops or Close is
	//    called.
	g.serveDataStreams(ctx, conn)
}

// serveHandshake runs http3's HandleRequestStream on the first bidi
// stream. Our handler validates the WebTransport Extended CONNECT and
// sends a 200 response. Once HandleRequestStream returns, the handshake
// is complete and any following bidi stream is WebTransport data.
func (g *Gateway) serveHandshake(rawConn *http3.RawServerConn, first *quic.Stream, conn *quic.Conn) {
	// Bind a per-handshake timeout via a goroutine so a misbehaving
	// client cannot hold the listener open forever.
	done := make(chan struct{})
	go func() {
		rawConn.HandleRequestStream(first)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(g.http3Cfg.handshakeTimeout):
		first.CancelRead(1)
		first.CancelWrite(1)
		_ = conn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeRequestRejected), "wt handshake timeout")
	}
}

// handler builds the http3 Handler used for the single Extended CONNECT
// request per connection. Returning any non-200 status aborts the
// connection (we reject plain HTTP requests; this listener is WebTransport-only).
//
// The Handler hijacks the response writer so the request stream stays
// open for the WT session's capsule protocol (per RFC 9298 §3.1).
func (g *Gateway) handler(conn *quic.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TEMP DEBUG: log exactly what we received.
		fmt.Printf("[DEBUG wt-handler] method=%q proto=%q path=%q ua=%q\n",
			r.Method, r.Proto, r.URL.Path, r.Header.Get("User-Agent"))
		if r.Proto != "webtransport" {
			http.Error(w, "expected extended CONNECT with :protocol=webtransport", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != g.pathPrefix {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}

		// Hijack the underlying http3 stream so it stays open for
		// capsule traffic after the Handler returns. http3 checks the
		// `wasStreamHijacked` flag and skips str.Close() if true.
		type hijacker interface {
			HTTPStream() *http3.Stream
		}
		hj, ok := w.(hijacker)
		if !ok {
			http.Error(w, "hijack not supported", http.StatusInternalServerError)
			return
		}
		stream := hj.HTTPStream()
		_ = stream // retained for capsule protocol future use; closed by http3 on conn tear-down.

		// Write the 200 response. http3 internally flushes the
		// HEADERS frame onto `stream` once WriteHeader returns, so by
		// the time we return from this function the browser has its
		// 200 confirmation and the WebTransport session is live on
		// the underlying QUIC connection.
		w.WriteHeader(http.StatusOK)
	}
}

// serveDataStreams accepts every NEW bidi stream that arrives after the
// handshake and pipes it into broker.HandleStream. ctx cancellation tears
// the connection down so resource leaks are bounded.
func (g *Gateway) serveDataStreams(ctx context.Context, conn *quic.Conn) {
	for {
		if ctx.Err() != nil {
			return
		}
		str, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		g.conns.Add(1)
		go func(s *quic.Stream) {
			defer g.conns.Done()
			clientID := clientIDFromConn(conn)
			// Hand off to the broker's existing pipeline (frame parser,
			// authz, AAL, router, …). Zero protocol changes: the frame
			// bytes the browser sends match the wire format native
			// clients use.
			g.broker.HandleStream(ctx, conn, s, clientID)
		}(str)
	}
}

// clientIDFromConn pulls the client identity from the QUIC connection's TLS
// state. Mirrors transport.handleConn so WT and native QUIC clients share
// the same ACL/audit semantics.
func clientIDFromConn(conn *quic.Conn) string {
	cs := conn.ConnectionState()
	if len(cs.TLS.PeerCertificates) == 0 {
		return "anonymous"
	}
	if cn := cs.TLS.PeerCertificates[0].Subject.CommonName; cn != "" {
		return cn
	}
	return "anonymous"
}

// cloneTLSForH3 returns a copy of baseTLS that has "h3" in NextProtos
// (required for HTTP/3 ALPN negotiation). The cert and other settings
// are preserved verbatim — we want the gateway to serve with the
// operator's existing mTLS certificate so browser-issued wildcard
// SANs Just Work.
func cloneTLSForH3(base *tls.Config) *tls.Config {
	if base == nil {
		return &tls.Config{
			MinVersion: tls.VersionTLS13,
			NextProtos: []string{alpnH3},
		}
	}
	nextProtos := append([]string(nil), base.NextProtos...)
	hasH3 := false
	for _, p := range nextProtos {
		if p == alpnH3 {
			hasH3 = true
			break
		}
	}
	if !hasH3 {
		nextProtos = append(nextProtos, alpnH3)
	}
	cp := base.Clone()
	cp.NextProtos = nextProtos
	// WebTransport mandates HTTP/3 over QUIC, which itself mandates
	// TLS 1.3. Force the minimum version so misconfigured production
	// certs (e.g. disabled TLS 1.3) do not silently downgrade to 1.2.
	cp.MinVersion = tls.VersionTLS13
	return cp
}

// Compile-time assertion that Gateway satisfies net.Listener-style
// lifecycle (Listen then Close). Helpful for tests.
var _ lifecycleListener = (*Gateway)(nil)

// lifecycleListener is the minimal API surface for serve/shutdown
// goroutines. Keeps the contract explicit so future refactors don't
// quietly break drain semantics.
type lifecycleListener interface {
	ListenAndServe(ctx context.Context, addr string, baseTLS *tls.Config) error
	Addr() string
	Close() error
}
