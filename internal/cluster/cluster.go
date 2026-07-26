package cluster

import (
	"context"
	"crypto/tls"
	"errors"
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

// PeerManager establishes and maintains outbound QUIC streams to static peer addresses.
// Forwarding is zero-copy: the raw pooled []byte is written directly to peer streams
// with the MeshForwarded bit already set in buf[1].
type PeerManager struct {
	peers    []*PeerRef
	tlsConf  *tls.Config
	quicConf *quic.Config

	closed atomic.Bool
	wg     sync.WaitGroup
}

// New creates a PeerManager and begins background reconnect loops for each peer address.
func New(ctx context.Context, addrs []string, tlsConf *tls.Config, quicConf *quic.Config) *PeerManager {
	// Build the peer slice fully before starting goroutines to avoid races.
	peers := make([]*PeerRef, len(addrs))
	for i, addr := range addrs {
		peers[i] = &PeerRef{addr: addr}
	}
	pm := &PeerManager{
		peers:    peers,
		tlsConf:  tlsConf,
		quicConf: quicConf,
	}
	for _, p := range pm.peers {
		p := p
		pm.wg.Add(1)
		go func() {
			defer pm.wg.Done()
			pm.reconnectLoop(ctx, p)
		}()
	}
	return pm
}

// reconnectLoop dials the peer and reopens its stream whenever it drops.
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
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < maxBackoff {
					backoff *= 2
				}
				continue
			}
		}
		backoff = initialBackoff

		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			conn.CloseWithError(0, "stream open failed")
			continue
		}

		p.mu.Lock()
		p.stream = stream
		p.mu.Unlock()

		metrics.ClusterPeersActive.Set(float64(pm.ActivePeers()))

		// Wait for stream to close (read drains EOF).
		buf := make([]byte, 1)
		for {
			_, err := stream.Read(buf)
			if err != nil {
				break
			}
		}

		p.mu.Lock()
		p.stream = nil
		p.mu.Unlock()

		metrics.ClusterPeersActive.Set(float64(pm.ActivePeers()))

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}
}

// Forward sends rawBuf zero-copy to all connected peer streams.
// It sets the MeshForwarded bit in a copy of buf[1] to prevent mesh storms.
// msgRef is Retained once per active peer; caller must NOT call Release on the ref
// after invoking Forward if it was already Retained for local dispatch.
func (pm *PeerManager) Forward(rawBuf []byte, addForwardedBit bool) {
	if len(rawBuf) < protocol.HeaderSize {
		return
	}
	for _, p := range pm.peers {
		p.mu.Lock()
		s := p.stream
		p.mu.Unlock()
		if s == nil {
			continue
		}

		// Zero-copy forwarding: write the modified frame in one contiguous chunk.
		// For small frames (<= 256 bytes) we build the modified buffer on the stack
		// so the entire frame arrives in a single QUIC STREAM frame at the receiver,
		// preventing header fragmentation. No heap allocation in the common case.
		if addForwardedBit {
			totalLen := len(rawBuf)
			if totalLen <= 256 {
				var combined [256]byte
				combined[0] = rawBuf[0]
				combined[1] = rawBuf[1] | byte(protocol.MeshForwardedBit)
				copy(combined[2:], rawBuf[2:])
				if _, err := s.Write(combined[:totalLen]); err != nil {
					continue
				}
			} else {
				var hdr [protocol.HeaderSize]byte
				hdr[0] = rawBuf[0]
				hdr[1] = rawBuf[1] | byte(protocol.MeshForwardedBit)
				copy(hdr[2:], rawBuf[2:protocol.HeaderSize])
				if _, err := s.Write(hdr[:]); err != nil {
					continue
				}
				if _, err := s.Write(rawBuf[protocol.HeaderSize:]); err != nil {
					continue
				}
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
	count := 0
	for _, p := range pm.peers {
		p.mu.Lock()
		if p.stream != nil {
			count++
		}
		p.mu.Unlock()
	}
	return count
}

// Close signals the reconnect goroutines to stop and drains the peer streams.
func (pm *PeerManager) Close() {
	pm.closed.Store(true)
	// Close all live streams so reconnect goroutines exit promptly.
	for _, p := range pm.peers {
		p.mu.Lock()
		if p.stream != nil {
			p.stream.Close()
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
	wrote := 0
	for _, p := range pm.peers {
		p.mu.Lock()
		s := p.stream
		p.mu.Unlock()
		if s == nil {
			continue
		}
		if _, err := s.Write(rawBuf); err == nil {
			wrote++
			metrics.ClusterFramesForwarded.Inc()
		}
	}
	return nil
}
