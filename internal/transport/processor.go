package transport

import "sync"

const defaultMaxIdleTimeout = 30e9 // 30 seconds in nanoseconds for quic.Config

// readBufPool is a fixed-capacity buffer pool for zero-allocation reads.
// Buffers are retrieved with a pre-allocated capacity and returned after use.
// Only buffers that maintain the original capacity are returned to the pool;
// grown buffers are discarded (GC'd), which is the expected rare path.
type readBufPool struct {
	pool sync.Pool
}

var _rp = &readBufPool{
	pool: sync.Pool{
		New: func() any {
			b := make([]byte, 0, defaultReadBufSize)
			return &b
		},
	},
}

// GetBuf returns a byte slice with at least minCap capacity.
// The returned slice has length == minCap (ready for Read into buf[0:]).
func (p *readBufPool) GetBuf(minCap int) []byte {
	raw := p.pool.Get()
	if raw == nil {
		b := make([]byte, minCap)
		return b
	}
	bp := raw.(*[]byte)
	b := *bp
	if cap(b) < minCap {
		return make([]byte, minCap)
	}
	return b[:minCap]
}

// PutBuf returns a buffer to the pool. Only buffers with the default
// capacity are returned; others are left for GC.
func (p *readBufPool) PutBuf(b []byte) {
	if cap(b) < defaultReadBufSize {
		return
	}
	b = b[:cap(b)]
	_rp.pool.Put(&b)
}
