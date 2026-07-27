package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const (
	initialBackoff = 250 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

// PeerRef holds the QUIC stream and connection state for a remote peer.
type PeerRef struct {
	addr   string
	mu     sync.Mutex
	stream *quic.Stream
}

// peerSlice is an immutable snapshot of the peer list.
// Writers create a new slice and atomically swap the pointer; readers
// (Forward) grab the current pointer and iterate — no locks needed on the hot path.
type peerSlice struct {
	refs []*PeerRef
}

// PeerManager establishes and maintains outbound QUIC streams to peer addresses.
// Forwarding is zero-copy: the raw pooled []byte is written directly to peer streams
// with the MeshForwarded bit already set in buf[1].
// Dynamic peer management uses RCU (Read-Copy-Update): Forward reads the atomic
// pointer; AddPeer/RemovePeer create a new slice and swap atomically.
type PeerManager struct {
	peers atomic.Pointer[peerSlice]

	tlsConf  *tls.Config
	quicConf *quic.Config
	logger   *slog.Logger

	closed atomic.Bool
	wg     sync.WaitGroup

	mu      sync.Mutex // protects addrs set (write path only)
	addrSet map[string]context.CancelFunc
}

// New creates a PeerManager and begins background reconnect loops for each peer address.
func New(ctx context.Context, addrs []string, tlsConf *tls.Config, quicConf *quic.Config) *PeerManager {
	return NewWithLogger(ctx, addrs, tlsConf, quicConf, slog.Default())
}

// NewWithLogger creates a PeerManager with a custom logger.
func NewWithLogger(ctx context.Context, addrs []string, tlsConf *tls.Config, quicConf *quic.Config, logger *slog.Logger) *PeerManager {
	pm := &PeerManager{
		tlsConf:  tlsConf,
		quicConf: quicConf,
		logger:   logger,
		addrSet:  make(map[string]context.CancelFunc),
	}

	refs := make([]*PeerRef, 0, len(addrs))
	for _, addr := range addrs {
		ref := &PeerRef{addr: addr}
		refs = append(refs, ref)
		childCtx, cancel := context.WithCancel(ctx)
		pm.addrSet[addr] = cancel
		pm.wg.Add(1)
		go func() {
			defer pm.wg.Done()
			pm.reconnectLoop(childCtx, ref)
		}()
	}
	pm.peers.Store(&peerSlice{refs: refs})
	return pm
}

// reconnectLoop dials the peer and reopens its stream whenever it drops.
//
// Refactored to keep cognitive complexity low: each step is its own helper.
func (pm *PeerManager) reconnectLoop(ctx context.Context, p *PeerRef) {
	backoff := initialBackoff
	for {
		if pm.closed.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := quic.DialAddr(ctx, p.addr, pm.tlsConf, pm.quicConf)
		if err != nil {
			if pm.waitBackoffOrDone(ctx, &backoff) {
				return
			}
			continue
		}
		backoff = initialBackoff

		if !pm.runPeerStream(ctx, p, conn) {
			return
		}
		if pm.waitBackoffOrDone(ctx, &backoff) {
			return
		}
	}
}

// waitBackoffOrDone sleeps for the backoff window or returns true if ctx is
// cancelled. The backoff doubles up to maxBackoff.
func (pm *PeerManager) waitBackoffOrDone(ctx context.Context, backoff *time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(*backoff):
	}
	if *backoff < maxBackoff {
		*backoff *= 2
	}
	return false
}

// runPeerStream opens a stream, sets it as the active stream, drains reads
// until EOF, and clears the active stream. Returns false if ctx is cancelled.
func (pm *PeerManager) runPeerStream(ctx context.Context, p *PeerRef, conn *quic.Conn) bool {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "stream open failed")
		return true
	}

	p.mu.Lock()
	p.stream = stream
	p.mu.Unlock()
	metrics.ClusterPeersActive.Set(float64(pm.ActivePeers()))

	buf := make([]byte, 1)
	for {
		if _, rerr := stream.Read(buf); rerr != nil {
			break
		}
	}

	p.mu.Lock()
	p.stream = nil
	p.mu.Unlock()
	metrics.ClusterPeersActive.Set(float64(pm.ActivePeers()))
	return true
}

// AddPeer dynamically adds a new peer address and starts its reconnect loop.
// Safe for concurrent use; uses RCU to swap the peer snapshot atomically.
func (pm *PeerManager) AddPeer(ctx context.Context, addr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed.Load() {
		return
	}
	if _, exists := pm.addrSet[addr]; exists {
		return // already managed
	}

	ref := &PeerRef{addr: addr}
	childCtx, cancel := context.WithCancel(ctx)
	pm.addrSet[addr] = cancel

	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		pm.reconnectLoop(childCtx, ref)
	}()

	pm.swapPeers(func(old []*PeerRef) []*PeerRef {
		refs := make([]*PeerRef, 0, len(old)+1)
		refs = append(refs, old...)
		refs = append(refs, ref)
		return refs
	})

	pm.logger.Info("peer added", "addr", addr)
}

// RemovePeer dynamically removes a peer address, stops its reconnect loop,
// and closes its active stream. Safe for concurrent use via RCU.
func (pm *PeerManager) RemovePeer(addr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	cancel, exists := pm.addrSet[addr]
	if !exists {
		return
	}
	// #nosec G104 -- context.CancelFunc has no error return; cancellation is idempotent.
	cancel()
	delete(pm.addrSet, addr)

	pm.swapPeers(func(old []*PeerRef) []*PeerRef {
		refs := make([]*PeerRef, 0, len(old))
		for _, p := range old {
			if p.addr == addr {
				p.mu.Lock()
				if p.stream != nil {
					_ = p.stream.Close() // #nosec G104 -- stream close errors during peer removal are non-actionable.
					p.stream = nil
				}
				p.mu.Unlock()
				continue
			}
			refs = append(refs, p)
		}
		return refs
	})

	pm.logger.Info("peer removed", "addr", addr)
}

// swapPeers applies fn to the current peer list and atomically stores the result.
func (pm *PeerManager) swapPeers(fn func([]*PeerRef) []*PeerRef) {
	old := pm.peers.Load()
	newRefs := fn(old.refs)
	pm.peers.Store(&peerSlice{refs: newRefs})
	metrics.ClusterPeersActive.Set(float64(pm.ActivePeers()))
}

// Forward sends rawBuf zero-copy to all connected peer streams.
// It sets the MeshForwarded bit in a copy of buf[1] to prevent mesh storms.
// RCU: reads the atomic pointer — no locks, no allocations on the hot path.
func (pm *PeerManager) Forward(rawBuf []byte, addForwardedBit bool) {
	if len(rawBuf) < protocol.HeaderSize {
		return
	}
	snap := pm.peers.Load()
	for _, p := range snap.refs {
		p.mu.Lock()
		s := p.stream
		p.mu.Unlock()
		if s == nil {
			continue
		}

		// Zero-copy forwarding: set the MeshForwarded bit in-place, write, then
		// restore the original byte. rawBuf comes from the caller (which releases
		// it via sync.Pool), so temporary mutation is safe and avoids heap allocation
		// of a separate buffer. No heap allocation in the common case.
		if addForwardedBit {
			orig := rawBuf[1]
			rawBuf[1] = orig | byte(protocol.MeshForwardedBit)
			_, werr := s.Write(rawBuf)
			rawBuf[1] = orig
			if werr != nil {
				continue
			}
		} else {
			if _, err := s.Write(rawBuf); err != nil {
				continue
			}
		}
		metrics.ClusterFramesForwarded.Inc()
	}
}

// ActivePeers returns the number of currently connected peers.
func (pm *PeerManager) ActivePeers() int {
	snap := pm.peers.Load()
	count := 0
	for _, p := range snap.refs {
		p.mu.Lock()
		if p.stream != nil {
			count++
		}
		p.mu.Unlock()
	}
	return count
}

// PeerCount returns the total number of managed peers (connected or not).
func (pm *PeerManager) PeerCount() int {
	snap := pm.peers.Load()
	return len(snap.refs)
}

// Close signals the reconnect goroutines to stop and drains the peer streams.
func (pm *PeerManager) Close() {
	pm.closed.Store(true)

	pm.mu.Lock()
	for _, cancel := range pm.addrSet {
		// #nosec G104 -- context.CancelFunc has no error return; cancellation is idempotent.
		cancel()
	}
	pm.addrSet = make(map[string]context.CancelFunc)
	pm.mu.Unlock()

	snap := pm.peers.Load()
	for _, p := range snap.refs {
		p.mu.Lock()
		if p.stream != nil {
			_ = p.stream.Close() // #nosec G104 -- stream close errors during shutdown are non-actionable.
			p.stream = nil
		}
		p.mu.Unlock()
	}
	pm.wg.Wait()
	metrics.ClusterPeersActive.Set(0)
}

// ForwardRaw sends a pre-assembled raw byte slice to all connected peers,
// treating the MeshForwarded bit as already set in the caller's buffer.
// Used for in-test direct writes where the caller controls byte-level framing.
func (pm *PeerManager) ForwardRaw(rawBuf []byte) error {
	if len(rawBuf) < protocol.HeaderSize {
		return errors.New("buffer too short for header")
	}
	snap := pm.peers.Load()
	for _, p := range snap.refs {
		p.mu.Lock()
		s := p.stream
		p.mu.Unlock()
		if s == nil {
			continue
		}
		if _, err := s.Write(rawBuf); err == nil {
			metrics.ClusterFramesForwarded.Inc()
		}
	}
	return nil
}
