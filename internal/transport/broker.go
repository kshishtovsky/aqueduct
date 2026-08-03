package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/tracing"
	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultReadBufSize              = 1024
	defaultMaxBufSize               = 64 * 1024
	defaultMaxDecompressedPerStream = 16 * 1024 * 1024 // 16 MB per stream
)

var (
	prefixTopic                 = []byte("topic:")
	errOversizedPayload         = errors.New("payload length exceeds maxBufSize")
	errBufferExceeded           = errors.New("unread buffer exceeds maxBufSize")
	errUnauthorized             = errors.New("authorization denied")
	errFrameDecompressionFailed = errors.New("frame decompression failed")
)

// Handler processes a parsed frame and returns an optional response payload.
type Handler func(ctx context.Context, frame protocol.Frame) ([]byte, error)

// Broker is a QUIC-based message broker that accepts connections and
// multiplexes streams. Each stream is handled by a dedicated goroutine that
// reads binary frames, parses them via protocol.ParseFrame, and dispatches
// to the registered command handler or the built-in router.
type Broker struct {
	listener *quic.Listener
	handlers map[protocol.Command]Handler
	router   *broker.Router
	aal      *aal.Log
	authz    *authz.Engine
	aalPath  string
	aalKey   []byte

	wg      sync.WaitGroup
	cancel  context.CancelFunc
	stopped atomic.Bool

	readBufSize int
	maxBufSize  int

	tracer *tracing.Tracer
	logger *slog.Logger

	compression        broker.CompressionEngine
	compressionMinSize int

	maxDecompressedPerStream int
}

// Option configures the Broker.
type Option func(*Broker)

// WithReadBufSize sets the initial read buffer size per stream processor.
// Must be >= protocol.HeaderSize (10). Default: 1024.
func WithReadBufSize(n int) Option {
	return func(b *Broker) { b.readBufSize = n }
}

// WithMaxBufSize sets the maximum buffer size before the processor falls back
// to heap allocation. Default: 64KB.
func WithMaxBufSize(n int) Option {
	return func(b *Broker) { b.maxBufSize = n }
}

// WithLogger sets a structured logger for the broker.
func WithLogger(l *slog.Logger) Option {
	return func(b *Broker) { b.logger = l }
}

// WithRouter sets the pub/sub router for CmdPublish and CmdSubscribe handling.
func WithRouter(r *broker.Router) Option {
	return func(b *Broker) { b.router = r }
}

// WithTracer sets the OpenTelemetry tracer for distributed tracing.
func WithTracer(t *tracing.Tracer) Option {
	return func(b *Broker) { b.tracer = t }
}

// WithAAL enables Append-Only Logging for CmdPublish frames.
func WithAAL(l *aal.Log) Option {
	return func(b *Broker) { b.aal = l }
}

// WithAALReplay enables startup state restoration from an AAL log file.
func WithAALReplay(path string, key []byte) Option {
	return func(b *Broker) {
		b.aalPath = path
		b.aalKey = key
	}
}

// WithAuthz enables the zero-allocation ACL authorization engine.
func WithAuthz(e *authz.Engine) Option {
	return func(b *Broker) { b.authz = e }
}

// WithCompression enables batch payload decompression for received frames.
// engine provides ZSTD decompression. minBatchSize is ignored on the receive side
// (all compressed batches are decompressed regardless of size).
func WithCompression(engine broker.CompressionEngine, minBatchSize int) Option {
	return func(b *Broker) {
		b.compression = engine
		b.compressionMinSize = minBatchSize
	}
}

// WithMaxDecompressedPerStream sets the maximum total decompressed bytes per
// stream before the broker rejects further compressed frames. Default: 16 MB.
// Prevents a single malicious peer from exhausting memory via compression bombs.
func WithMaxDecompressedPerStream(n int) Option {
	return func(b *Broker) { b.maxDecompressedPerStream = n }
}

// New creates a Broker with the given options. Call Listen to start accepting.
func New(opts ...Option) *Broker {
	b := &Broker{
		handlers:                 make(map[protocol.Command]Handler),
		readBufSize:              defaultReadBufSize,
		maxBufSize:               defaultMaxBufSize,
		maxDecompressedPerStream: defaultMaxDecompressedPerStream,
		logger:                   slog.Default(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// ReplayAAL replays all historical CmdPublish frames from AAL into the router before socket bind.
func (b *Broker) ReplayAAL(ctx context.Context, path string, key []byte) (int, error) {
	if path == "" {
		return 0, nil
	}
	t0 := time.Now()
	defer func() {
		metrics.AALReplayDuration.Set(time.Since(t0).Seconds())
	}()

	count, err := aal.Replay(path, key, func(frameBytes []byte) error {
		frame, parseErr := protocol.ParseFrame(frameBytes)
		if parseErr != nil {
			return parseErr
		}
		if frame.Command == protocol.CmdPublish && b.router != nil {
			return b.router.Publish(ctx, frame)
		}
		return nil
	})
	if count > 0 {
		b.logger.Info("AAL replay completed", "records_replayed", count)
	}
	return count, err
}

// Handle registers a handler for the given command. Must be called before Listen.
func (b *Broker) Handle(cmd protocol.Command, h Handler) {
	b.handlers[cmd] = h
}

// Listen starts the QUIC listener on the given UDP address with 0-RTT support.
// tlsConf must contain a valid certificate and set NextProtos.
func (b *Broker) Listen(ctx context.Context, addr string, tlsConf *tls.Config) error {
	if b.aalPath != "" {
		if _, err := b.ReplayAAL(ctx, b.aalPath, b.aalKey); err != nil {
			b.logger.Warn("AAL replay encountered error", "err", err)
		}
	}

	quicConf := &quic.Config{
		Allow0RTT:          true,
		MaxIdleTimeout:     defaultMaxIdleTimeout,
		MaxIncomingStreams: 100,
	}

	ln, err := quic.ListenAddr(addr, tlsConf, quicConf)
	if err != nil {
		return err
	}

	b.listener = ln
	jig, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	b.logger.Info("broker listening", "addr", ln.Addr().String())

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		runAcceptLoop(b, jig, ln)
	}()

	return nil
}

// Addr returns the listener's network address. Returns nil if not listening.
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// AuthzEngine returns the active authorization engine.
func (b *Broker) AuthzEngine() *authz.Engine {
	return b.authz
}

// SetAuthzEngine updates the active authorization engine.
func (b *Broker) SetAuthzEngine(e *authz.Engine) {
	b.authz = e
}

// Router returns the pub/sub router.
func (b *Broker) Router() *broker.Router {
	return b.router
}

// runAcceptLoop accepts incoming QUIC connections and spawns stream processors.
func runAcceptLoop(b *Broker, jig context.Context, ln *quic.Listener) {
	for {
		conn, err := ln.Accept(jig)
		if err != nil {
			if jig.Err() != nil {
				return
			}
			b.logger.Error("accept error", "err", err)
			return
		}

		b.logger.Info("connection accepted",
			"remote", conn.RemoteAddr(),
			"0rtt", conn.ConnectionState().Used0RTT,
		)

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			runHandleConn(b, jig, conn)
		}()
	}
}

// runHandleConn processes all streams on a single QUIC connection.
func runHandleConn(b *Broker, jig context.Context, conn *quic.Conn) {
	cs := conn.ConnectionState()
	clientIDStr := "anonymous"
	if len(cs.TLS.PeerCertificates) > 0 {
		if cn := cs.TLS.PeerCertificates[0].Subject.CommonName; cn != "" {
			clientIDStr = cn
		}
	}

	go func() {
		<-jig.Done()
		_ = conn.CloseWithError(0, "broker shutdown")
	}()

	for {
		stream, err := conn.AcceptStream(jig)
		if err != nil {
			if jig.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
				return
			}
			var appErr *quic.ApplicationError
			if errors.As(err, &appErr) && appErr.ErrorCode == 0 {
				b.logger.Debug("client disconnected", "remote", conn.RemoteAddr())
				return
			}
			b.logger.Warn("accept stream error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.processStream(jig, conn, stream, clientIDStr)
		}()
	}
}

// processStream reads frames from a single QUIC stream and dispatches them.
func (b *Broker) processStream(jig context.Context, conn *quic.Conn, stream *quic.Stream, clientIDStr string) {
	b.HandleStream(jig, conn, stream, clientIDStr)
}

// HandleStream is the public, transport-agnostic entry point for feeding a
// single bidirectional QUIC stream through the broker's frame parser,
// authorization engine, AAL log, and router. It is safe to call from any
// goroutine; concurrency control is handled inside transport.Broker.
//
// Use this when integrating new transports (e.g. internal/webtransport) that
// ride on top of QUIC bi-di streams without spawning an extra transport.Broker.
func (b *Broker) HandleStream(jig context.Context, conn *quic.Conn, stream *quic.Stream, clientIDStr string) {
	span := b.newStreamSpan(stream, conn, clientIDStr)

	// Ensure cleanup on stream close.
	defer func() {
		if b.router != nil {
			// #nosec G115 -- StreamID is bounded by the QUIC stream ID space (<<2^63); the router keys by uint32 hash of the low bits.
			b.router.Unsubscribe(uint32(stream.StreamID()))
		}
	}()

	buf := _rp.GetBuf(b.readBufSize)
	defer _rp.PutBuf(buf)

	// Per-stream decompressed byte counter for compression bomb protection.
	var decompressedBytes int64

	b.runStreamReadLoop(jig, span, stream, clientIDStr, buf, &decompressedBytes)
}

// runStreamReadLoop drives the read-dispatch-grow cycle. Each iteration:
//  1. dispatch any complete frames already buffered;
//  2. grow the buffer if it filled up after dispatch;
//  3. read more bytes from the QUIC stream.
//
// Errors from dispatch (oversized payload, unauthorized) cause a CancelRead/Write
// and return. Errors from stream.Read on a closed stream are silent.
func (b *Broker) runStreamReadLoop(jig context.Context, span *slog.Logger, stream *quic.Stream, clientIDStr string, buf []byte, decompressedBytes *int64) {
	off := 0
	for {
		if off == cap(buf) {
			if !b.drainAndMaybeGrow(jig, span, stream, clientIDStr, &buf, &off, decompressedBytes) {
				return
			}
		}

		n, err := stream.Read(buf[off:])
		if n > 0 {
			off += n
			newOff, dispatchErr := b.dispatchFrames(jig, span, buf[:off], off, stream, clientIDStr, decompressedBytes)
			if dispatchErr != nil {
				b.handleDispatchError(span, stream, dispatchErr)
				return
			}
			off = newOff
		}
		if err != nil {
			if b.isExpectedStreamReadError(err) {
				return
			}
			span.Warn("stream read error", "err", err)
			return
		}
	}
}

// drainAndMaybeGrow dispatches the buffered complete frames and, if the buffer
// is still full, doubles its capacity up to b.maxBufSize. Returns false when
// the stream must be torn down (oversized payload, unauthorized, hit max).
func (b *Broker) drainAndMaybeGrow(jig context.Context, span *slog.Logger, stream *quic.Stream, clientIDStr string, buf *[]byte, off *int, decompressedBytes *int64) bool {
	newOff, err := b.dispatchFrames(jig, span, (*buf)[:*off], *off, stream, clientIDStr, decompressedBytes)
	if err != nil {
		b.handleDispatchError(span, stream, err)
		return false
	}
	*off = newOff
	if *off != cap(*buf) {
		return true
	}
	if cap(*buf) >= b.maxBufSize {
		span.Warn("unread buffer size exceeded maxBufSize", "off", *off, "max_buf_size", b.maxBufSize)
		stream.CancelRead(1)
		stream.CancelWrite(1)
		return false
	}
	b.growBuffer(buf)
	return true
}

// growBuffer doubles *buf's capacity up to b.maxBufSize, copying the live data.
func (b *Broker) growBuffer(buf *[]byte) {
	newCap := cap(*buf) * 2
	if newCap > b.maxBufSize {
		newCap = b.maxBufSize
	}
	newBuf := make([]byte, newCap)
	copy(newBuf, (*buf)[:cap(*buf)])
	*buf = newBuf
}

// handleDispatchError centralizes the dispatch-error paths: unauthorized
// uses stream error 401, oversized / memory uses 1. Both cancel read+write
// and rely on the loop caller to return.
func (b *Broker) handleDispatchError(span *slog.Logger, stream *quic.Stream, err error) {
	if errors.Is(err, errUnauthorized) {
		stream.CancelRead(401)
		stream.CancelWrite(401)
		return
	}
	span.Warn("oversized payload or memory limit exceeded", "err", err)
	stream.CancelRead(1)
	stream.CancelWrite(1)
}

// isExpectedStreamReadError returns true for errors that are normal during
// shutdown (server closed, conn closed, EOF) and do not warrant a log line.
func (b *Broker) isExpectedStreamReadError(err error) bool {
	if b.stopped.Load() || errors.Is(err, quic.ErrServerClosed) {
		return true
	}
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)
}

// newStreamSpan builds the per-stream logger pre-bound with the standard
// stream_id / remote / client_id fields.
func (b *Broker) newStreamSpan(stream *quic.Stream, conn *quic.Conn, clientIDStr string) *slog.Logger {
	return b.logger.With(
		"stream_id", stream.StreamID(),
		"remote", conn.RemoteAddr(),
		"client_id", clientIDStr,
	)
}

// dispatchFrames splits buf[:off] into complete frames and invokes handlers
// or the built-in router. Returns the new offset after consuming all complete
// frames (preserving any trailing partial frame bytes for the next read).
//
// Refactored into a slim orchestrator: header/decompress/authz/AAL are
// handled by prepareFrame, routing decisions by routeFrame. Each complete
// frame is parsed exactly once.
func (b *Broker) dispatchFrames(jig context.Context, logger *slog.Logger, buf []byte, off int, stream *quic.Stream, clientIDStr string, decompressedBytes *int64) (int, error) {
	consumed := 0
	for consumed < off {
		frame, totalLen, partial, err := b.prepareFrame(jig, logger, buf, off, consumed, stream, clientIDStr, decompressedBytes)
		if err != nil {
			return 0, err
		}
		if partial {
			break
		}
		b.routeFrame(jig, logger, stream, frame, clientIDStr)
		consumed += totalLen
	}

	n := copy(buf, buf[consumed:off])
	return n, nil
}

// prepareFrame validates the header, decompresses (if a Compression TLV is
// present), runs authorization and append-only logging for publish frames.
// It returns the parsed frame (zero-copy header view into the buffer) plus:
//
//	totalLen — the wire length to advance the buffer by (0 when partial)
//	partial  — true when the buffer holds no complete frame; caller breaks
//	err      — non-nil on protocol/auth/decompression failures
func (b *Broker) prepareFrame(jig context.Context, logger *slog.Logger, buf []byte, off int, consumed int, stream *quic.Stream, clientIDStr string, decompressedBytes *int64) (protocol.Frame, int, bool, error) {
	remaining := off - consumed
	if remaining < protocol.HeaderSize {
		if remaining > b.maxBufSize {
			return protocol.Frame{}, 0, false, errBufferExceeded
		}
		return protocol.Frame{}, 0, true, nil
	}

	payloadLen := protocol.PayloadLen(buf[consumed:])
	totalLen := protocol.HeaderSize + int(payloadLen)
	if int(payloadLen) > b.maxBufSize || totalLen > b.maxBufSize {
		return protocol.Frame{}, 0, false, errOversizedPayload
	}
	if remaining < totalLen {
		return protocol.Frame{}, 0, true, nil
	}

	frame, perr := protocol.ParseFrame(buf[consumed:])
	if perr != nil {
		logger.Warn("frame parse error", "err", perr)
		return protocol.Frame{}, 0, false, perr
	}

	// Decompress payload if Compression TLV extension is present.
	// This must happen before authorization and routing.
	_, extToRelease, derr := b.decompressFrame(frame, decompressedBytes)
	if derr != nil {
		logger.Warn("frame decompression error", "err", derr)
		stream.CancelRead(1)
		stream.CancelWrite(1)
		return protocol.Frame{}, 0, false, errFrameDecompressionFailed
	}
	if extToRelease != nil {
		defer protocol.ReleaseExtensions(extToRelease)
	}

	// Authorization check.
	if b.authz != nil {
		requiredPerm := permissionForFrame(frame.Command)
		if requiredPerm != authz.PermNone {
			topicBytes := extractTopicBytes(frame.Payload)
			if !b.authz.Allowed(clientIDStr, topicBytes, requiredPerm) {
				metrics.AuthzDenied.WithLabelValues(metrics.SanitizeClient(clientIDStr), metrics.SanitizeTopic(string(topicBytes))).Inc()
				return protocol.Frame{}, 0, false, errUnauthorized
			}
		}
	}

	// Append-Only Logging for CmdPublish and CmdPublishBatch.
	if (frame.Command == protocol.CmdPublish || frame.Command == protocol.CmdPublishBatch) && b.aal != nil {
		if werr := b.aal.WriteFrame(buf[consumed : consumed+totalLen]); werr != nil {
			logger.Warn("aal write error", "err", werr)
		}
	}
	return frame, totalLen, false, nil
}

// routeFrame dispatches a parsed frame through either a peer-forwarder
// (when MeshForwarded is set) or the local router (subscribe/publish/ack/nack)
// or the legacy handler map.
func (b *Broker) routeFrame(jig context.Context, logger *slog.Logger, stream *quic.Stream, frame protocol.Frame, clientIDStr string) {
	if b.router != nil {
		if protocol.IsForwarded(frame.Command) {
			b.handleForwardedFrame(jig, logger, frame)
			return
		}
		if b.handleLocalFrame(jig, logger, stream, frame, clientIDStr) {
			return
		}
	}

	h, ok := b.handlers[frame.Command]
	if !ok {
		logger.Warn("no handler for command", "cmd", frame.Command)
		return
	}
	resp, err := h(jig, frame)
	if err != nil {
		logger.Warn("handler error", "cmd", frame.Command, "err", err)
		return
	}
	if resp != nil {
		b.sendResponse(stream, frame.StreamID, resp)
	}
}

// handleForwardedFrame dispatches a frame received from a peer (MeshForwarded
// bit set) to the local router without re-forwarding to other peers.
func (b *Broker) handleForwardedFrame(jig context.Context, logger *slog.Logger, frame protocol.Frame) {
	switch protocol.OpcodeOf(frame.Command) {
	case protocol.CmdPublish:
		publishCtx, endSpan := b.startPublishSpan(jig, tracing.SpanNameForward, frame, false)
		if err := b.router.PublishFromPeer(publishCtx, frame); err != nil {
			logger.Warn("peer publish error", "err", err)
		}
		endSpan()
	case protocol.CmdPublishBatch:
		publishCtx, endSpan := b.startPublishSpan(jig, tracing.SpanNameForward, frame, true)
		if err := b.router.PublishBatch(publishCtx, frame); err != nil {
			logger.Warn("peer batch publish error", "err", err)
		}
		endSpan()
	}
}

// handleLocalFrame dispatches local (non-forwarded) pub/sub/ack/nack frames
// to the router. Returns true if the command matched a router opcode, false
// when routeFrame should fall through to the legacy handler map.
func (b *Broker) handleLocalFrame(jig context.Context, logger *slog.Logger, stream *quic.Stream, frame protocol.Frame, clientIDStr string) bool {
	switch protocol.OpcodeOf(frame.Command) {
	case protocol.CmdSubscribe:
		if err := b.router.Subscribe(jig, stream, frame); err != nil {
			logger.Warn("subscribe error", "err", err)
		}
		return true
	case protocol.CmdPublish:
		publishCtx, endSpan := b.startPublishSpan(jig, tracing.SpanNameProcess, frame, false)
		if err := b.router.PublishWithClientID(publishCtx, frame, clientIDStr); err != nil {
			logger.Warn("publish error", "err", err)
		}
		endSpan()
		return true
	case protocol.CmdPublishBatch:
		publishCtx, endSpan := b.startPublishSpan(jig, tracing.SpanNameProcess, frame, true)
		if err := b.router.PublishBatch(publishCtx, frame); err != nil {
			logger.Warn("batch publish error", "err", err)
		}
		endSpan()
		return true
	case protocol.CmdAck:
		if consumerID, topic, offset, err := parseAckPayload(frame.Payload); err == nil {
			b.router.AckOffset(consumerID, topic, offset)
		} else {
			logger.Warn("ack payload error", "err", err)
		}
		return true
	case protocol.CmdNack:
		if len(frame.Payload) < 8 {
			logger.Warn("nack payload too short", "len", len(frame.Payload))
			return true
		}
		nackOffset := binary.LittleEndian.Uint64(frame.Payload[:8])
		// #nosec G115 -- stream.StreamID() is a quic int64; the router hashes low 32 bits which is sufficient for the per-stream map.
		b.router.NackByStream(uint32(stream.StreamID()), nackOffset)
		return true
	}
	return false
}

// startPublishSpan is the zero-allocation tracer wrapper used by every
// publish path. When tracing is disabled or the frame has no W3C Trace
// Context extension, it returns the original context and a shared no-op
// finish closure.
func (b *Broker) startPublishSpan(jig context.Context, spanName string, frame protocol.Frame, batch bool) (context.Context, func()) {
	if b.tracer == nil || !frame.HasExtensions() {
		return jig, noopSpan
	}
	traceID, spanID, traceFlags, ok := protocol.ExtractTraceContext(frame.Extensions)
	if !ok {
		return jig, noopSpan
	}
	attrs := []attribute.KeyValue{attribute.Int("stream_id", int(frame.StreamID))}
	if batch {
		attrs = append(attrs, attribute.String("messaging.operation", "batch_publish"))
	}
	ctx, end := b.tracer.StartSpanWithTraceContext(jig, spanName,
		traceID, spanID, traceFlags, trace.WithAttributes(attrs...))
	metrics.TracingSpansTotal.Inc()
	return ctx, end
}

// noopSpan is a shared no-op span finish callback for the hot path. Returning
// the same function value avoids allocating a fresh closure per publish.
func noopSpan() {}

// permissionForFrame maps a wire command to the ACL permission it requires.
// Returns PermNone for commands that are not ACL-gated.
func permissionForFrame(cmd protocol.Command) authz.Permission {
	switch cmd {
	case protocol.CmdPublish, protocol.CmdPublishBatch:
		return authz.PermPublish
	case protocol.CmdSubscribe:
		return authz.PermSubscribe
	default:
		return authz.PermNone
	}
}

func (b *Broker) sendResponse(stream *quic.Stream, streamID uint32, payload []byte) {
	if stream == nil {
		return
	}
	buf := protocol.SerializeFrame(protocol.CmdAck, streamID, payload)
	defer protocol.ReleaseBuffer(buf)
	if _, err := stream.Write(*buf); err != nil {
		b.logger.Warn("send response error", "stream_id", streamID, "err", err)
	}
}

func extractTopicBytes(payload []byte) []byte {
	if len(payload) >= 6 && bytes.HasPrefix(payload, prefixTopic) {
		return payload[6:]
	}
	return payload
}

// Close initiates a graceful shutdown of the Broker.
func (b *Broker) Close() error {
	if b.stopped.Swap(true) {
		return nil
	}
	if b.cancel != nil {
		b.cancel()
	}
	if b.listener != nil {
		return b.listener.Close()
	}
	return nil
}

// Shutdown gracefully drains active connections and goroutines.
func (b *Broker) Shutdown(ctx context.Context) error {
	if !b.stopped.CompareAndSwap(false, true) {
		return nil
	}

	b.logger.Info("shutdown initiated")

	if b.aal != nil {
		if err := b.aal.Close(); err != nil {
			b.logger.Warn("aal close error", "err", err)
		}
	}

	if b.cancel != nil {
		b.cancel()
	}

	var closeErr error
	if b.listener != nil {
		closeErr = b.listener.Close()
	}

	waitDone := make(chan struct{})
	wg := &b.wg
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		b.logger.Info("shutdown complete")
	case <-ctx.Done():
		b.logger.Warn("shutdown forced", "err", ctx.Err())
		return ctx.Err()
	}

	return closeErr
}

func parseAckPayload(payload []byte) (consumerID, topic string, offset uint64, err error) {
	str := string(payload)
	parts := strings.Split(str, ":")
	if len(parts) >= 6 && parts[0] == "topic" && parts[2] == "consumer" && parts[4] == "offset" {
		topic = parts[1]
		consumerID = parts[3]
		offset, err = strconv.ParseUint(parts[5], 10, 64)
		return consumerID, topic, offset, err
	}
	return "", "", 0, errors.New("invalid ack payload format")
}

// decompressFrame checks for the Compression TLV extension and decompresses
// the frame payload if present. Returns the frame unchanged if no compression.
// On decompression, strips the Compression TLV from extensions so downstream
// routing does not see it. The second return value is a slab-allocated extension
// block that the caller must ReleaseExtensions when routing is complete.
func (b *Broker) decompressFrame(frame protocol.Frame, decompressedBytes *int64) (protocol.Frame, []byte, error) {
	if b.compression == nil {
		return frame, nil, nil
	}
	extVal, found := protocol.FindExtension(frame.Extensions, protocol.ExtCompression)
	if !found || len(extVal) < protocol.ExtCompressionValueLen {
		return frame, nil, nil
	}

	algo := extVal[0]
	// #nosec G115 -- uncompressedSize is a wire uint32 from the TLV; the max-batch-size check below bounds it before further use.
	uncompressedSize := binary.LittleEndian.Uint32(extVal[1:5])

	if algo != protocol.AlgoZSTD || uncompressedSize == 0 {
		return frame, nil, nil
	}
	if uncompressedSize > uint32(b.maxBufSize)*16 { // #nosec G115 -- maxBufSize is operator-configured and bounded (< 2^28 default 64KB).
		return frame, nil, errors.New("decompressed size exceeds max limit")
	}

	// Per-stream decompression byte budget: reject if this stream has already
	// decompressed more than the configured limit. Prevents compression bombs
	// from exhausting server memory via repeated small compressed frames.
	if decompressedBytes != nil {
		newTotal := *decompressedBytes + int64(uncompressedSize)
		if newTotal > int64(b.maxDecompressedPerStream) {
			return frame, nil, errors.New("per-stream decompressed byte limit exceeded")
		}
		*decompressedBytes = newTotal
	}

	decompressed, err := b.compression.Decompress(frame.Payload, int(uncompressedSize))
	if err != nil {
		return frame, nil, err
	}

	cleanExt := protocol.StripExtension(frame.Extensions, protocol.ExtCompression)

	return protocol.Frame{
		Command:    frame.Command,
		StreamID:   frame.StreamID,
		PayloadLen: uncompressedSize,
		Payload:    decompressed,
		Extensions: cleanExt,
	}, cleanExt, nil
}
