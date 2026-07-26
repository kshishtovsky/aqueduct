package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const (
	defaultReadBufSize = 1024
	defaultMaxBufSize  = 64 * 1024
)

var (
	errOversizedPayload = errors.New("payload length exceeds maxBufSize")
	errBufferExceeded   = errors.New("unread buffer exceeds maxBufSize")
	errUnauthorized     = errors.New("authorization denied")
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

	wg      sync.WaitGroup
	cancel  context.CancelFunc
	stopped atomic.Bool

	readBufSize int
	maxBufSize  int

	logger *slog.Logger
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

// WithAAL enables Append-Only Logging for CmdPublish frames.
func WithAAL(l *aal.Log) Option {
	return func(b *Broker) { b.aal = l }
}

// WithAuthz enables the zero-allocation ACL authorization engine.
func WithAuthz(e *authz.Engine) Option {
	return func(b *Broker) { b.authz = e }
}

// New creates a Broker with the given options. Call Listen to start accepting.
func New(opts ...Option) *Broker {
	b := &Broker{
		handlers:    make(map[protocol.Command]Handler),
		readBufSize: defaultReadBufSize,
		maxBufSize:  defaultMaxBufSize,
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Handle registers a handler for the given command. Must be called before Listen.
func (b *Broker) Handle(cmd protocol.Command, h Handler) {
	b.handlers[cmd] = h
}

// Listen starts the QUIC listener on the given UDP address with 0-RTT support.
// tlsConf must contain a valid certificate and set NextProtos.
func (b *Broker) Listen(ctx context.Context, addr string, tlsConf *tls.Config) error {
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
	clientIDHash := authz.HashString(clientIDStr)

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
			b.logger.Warn("accept stream error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.processStream(jig, conn, stream, clientIDStr, clientIDHash)
		}()
	}
}

// processStream reads frames from a single QUIC stream and dispatches them.
func (b *Broker) processStream(jig context.Context, conn *quic.Conn, stream *quic.Stream, clientIDStr string, clientIDHash uint64) {
	streamID := stream.StreamID()
	span := b.logger.With(
		"stream_id", streamID,
		"remote", conn.RemoteAddr(),
		"client_id", clientIDStr,
	)

	// Ensure cleanup on stream close.
	defer func() {
		if b.router != nil {
			b.router.Unsubscribe(uint32(streamID))
		}
	}()

	buf := _rp.GetBuf(b.readBufSize)
	defer _rp.PutBuf(buf)

	off := 0

	for {
		if off == cap(buf) {
			var err error
			off, err = b.dispatchFrames(jig, span, buf[:off], off, stream, clientIDStr, clientIDHash)
			if err != nil {
				if errors.Is(err, errUnauthorized) {
					stream.CancelRead(401)
					stream.CancelWrite(401)
					return
				}
				span.Warn("oversized payload or memory limit exceeded", "err", err)
				stream.CancelRead(1)
				stream.CancelWrite(1)
				return
			}
			if off == cap(buf) {
				if cap(buf) >= b.maxBufSize {
					span.Warn("unread buffer size exceeded maxBufSize", "off", off, "max_buf_size", b.maxBufSize)
					stream.CancelRead(1)
					stream.CancelWrite(1)
					return
				}
				newCap := cap(buf) * 2
				if newCap > b.maxBufSize {
					newCap = b.maxBufSize
				}
				newBuf := make([]byte, newCap)
				copy(newBuf, buf[:off])
				buf = newBuf
			}
		}

		n, err := stream.Read(buf[off:])
		if n > 0 {
			off += n
			var dispatchErr error
			off, dispatchErr = b.dispatchFrames(jig, span, buf[:off], off, stream, clientIDStr, clientIDHash)
			if dispatchErr != nil {
				if errors.Is(dispatchErr, errUnauthorized) {
					stream.CancelRead(401)
					stream.CancelWrite(401)
					return
				}
				span.Warn("oversized payload or memory limit exceeded", "err", dispatchErr)
				stream.CancelRead(1)
				stream.CancelWrite(1)
				return
			}
		}
		if err != nil {
			if b.stopped.Load() || errors.Is(err, quic.ErrServerClosed) {
				return
			}
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			span.Warn("stream read error", "err", err)
			return
		}
	}
}

// dispatchFrames splits buf[:off] into complete frames and invokes handlers
// or the built-in router. Returns the new offset after consuming all complete
// frames (preserving any trailing partial frame bytes for the next read).
func (b *Broker) dispatchFrames(jig context.Context, logger *slog.Logger, buf []byte, off int, stream *quic.Stream, clientIDStr string, clientIDHash uint64) (int, error) {
	consumed := 0
	for consumed < off {
		remaining := off - consumed
		if remaining < protocol.HeaderSize {
			if remaining > b.maxBufSize {
				return 0, errBufferExceeded
			}
			break
		}

		payloadLen := protocol.PayloadLen(buf[consumed:])
		totalLen := protocol.HeaderSize + int(payloadLen)

		if int(payloadLen) > b.maxBufSize || totalLen > b.maxBufSize {
			return 0, errOversizedPayload
		}

		if remaining < totalLen {
			break
		}

		frame, err := protocol.ParseFrame(buf[consumed:])
		if err != nil {
			logger.Warn("frame parse error", "err", err)
			return 0, err
		}

		// Authorization check
		if b.authz != nil {
			var requiredPerm authz.Permission
			switch frame.Command {
			case protocol.CmdPublish:
				requiredPerm = authz.PermPublish
			case protocol.CmdSubscribe:
				requiredPerm = authz.PermSubscribe
			}
			if requiredPerm != authz.PermNone {
				topicBytes := extractTopicBytes(frame.Payload)
				if !b.authz.Allowed(clientIDHash, topicBytes, requiredPerm) {
					metrics.AuthzDenied.WithLabelValues(clientIDStr, string(topicBytes)).Inc()
					return 0, errUnauthorized
				}
			}
		}

		// Append-Only Logging for CmdPublish
		if frame.Command == protocol.CmdPublish && b.aal != nil {
			if err := b.aal.WriteFrame(buf[consumed : consumed+totalLen]); err != nil {
				logger.Warn("aal write error", "err", err)
			}
		}

		// Route pub/sub commands through the built-in router.
		if b.router != nil {
			switch frame.Command {
			case protocol.CmdSubscribe:
				if err := b.router.Subscribe(jig, stream, frame); err != nil {
					logger.Warn("subscribe error", "err", err)
				}
				consumed += totalLen
				continue
			case protocol.CmdPublish:
				if err := b.router.Publish(jig, frame); err != nil {
					logger.Warn("publish error", "err", err)
				}
				consumed += totalLen
				continue
			}
		}

		h, ok := b.handlers[frame.Command]
		if !ok {
			logger.Warn("no handler for command", "cmd", frame.Command)
			consumed += totalLen
			continue
		}

		resp, err := h(jig, frame)
		if err != nil {
			logger.Warn("handler error", "cmd", frame.Command, "err", err)
			consumed += totalLen
			continue
		}

		if resp != nil {
			b.sendResponse(stream, frame.StreamID, resp)
		}

		consumed += totalLen
	}

	n := copy(buf, buf[consumed:off])
	return n, nil
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
	if len(payload) >= 6 && string(payload[:6]) == "topic:" {
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

