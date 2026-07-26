package broker

import (
	"sync"
	"sync/atomic"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

var msgRefPool = sync.Pool{
	New: func() any {
		return &MessageRef{}
	},
}

// MessageRef wraps a pooled frame buffer (*[]byte) with an atomic reference counter, offset, and optional TTL expiry.
// It ensures that buffers recycled into sync.Pool are never reused while queued
// in another subscriber's pipeline or undergoing network I/O.
//
// Nested Reference Counting:
// When a batch buffer is unpacked, each sub-message gets its own MessageRef with
// a parent pointer pointing to the batch MessageRef. When a child ref count reaches 0,
// it decrements the parent. The batch buffer is returned to sync.Pool only when
// the parent ref count reaches 0 (all sub-messages from the batch have been delivered).
//
// For child refs, `frame` holds a zero-copy sub-slice of the parent's buffer.
// The child does not own the buffer; it delegates buffer lifecycle to the parent.
type MessageRef struct {
	buf       *[]byte
	frame     []byte // zero-copy sub-slice of parent buffer (for batch children)
	ref       atomic.Int32
	expiresAt int64  // unix nano timestamp, 0 = no expiry
	offset    uint64 // topic offset
	parent    *MessageRef
}

// AcquireMessageRef pulls a MessageRef from pool, wraps the frame buffer, and sets ref count to 1.
// The returned MessageRef has no parent (top-level owner of the buffer).
func AcquireMessageRef(buf *[]byte) *MessageRef {
	m := msgRefPool.Get().(*MessageRef)
	m.buf = buf
	m.frame = nil
	m.expiresAt = 0
	m.offset = 0
	m.parent = nil
	m.ref.Store(1)
	return m
}

// AcquireChildMessageRef creates a child MessageRef whose frame is a zero-copy sub-slice
// of the parent's buffer. The child's ref starts at 1. When released, it decrements the
// parent's ref count. The batch buffer is returned to sync.Pool only when the parent ref
// count reaches 0 (all sub-messages delivered).
//
// frame must point into the parent's underlying buffer memory. No copy is made.
func AcquireChildMessageRef(parent *MessageRef, frame []byte, topicOffset uint64, expiresAt int64) *MessageRef {
	m := msgRefPool.Get().(*MessageRef)
	m.buf = nil
	m.frame = frame
	m.expiresAt = expiresAt
	m.offset = topicOffset
	m.parent = parent
	m.ref.Store(1)
	// Retain the parent: each child holds one reference.
	parent.ref.Add(1)
	return m
}

// SetExpiresAt sets the unix nanosecond expiration timestamp.
func (m *MessageRef) SetExpiresAt(exp int64) {
	if m != nil {
		m.expiresAt = exp
	}
}

// SetOffset sets the 64-bit monotonic topic offset.
func (m *MessageRef) SetOffset(offset uint64) {
	if m != nil {
		m.offset = offset
	}
}

// Offset returns the message topic offset.
func (m *MessageRef) Offset() uint64 {
	if m == nil {
		return 0
	}
	return m.offset
}

// IsExpired checks if the message expiration time has passed.
func (m *MessageRef) IsExpired(nowNano int64) bool {
	return m != nil && m.expiresAt > 0 && nowNano >= m.expiresAt
}

// Retain increments the reference counter by 1.
func (m *MessageRef) Retain() {
	if m != nil {
		m.ref.Add(1)
	}
}

// Release decrements the reference counter by 1.
//
// If this is a child ref (has parent) and ref count drops to 0, it cascades:
// decrements the parent's ref count and recycles itself. The parent owns the
// real buffer and will return it to sync.Pool only when its own ref count reaches 0.
//
// If this is a top-level ref (no parent) and ref count drops to 0, the underlying
// buffer is returned to protocol.ReleaseBuffer and the MessageRef is recycled.
func (m *MessageRef) Release() {
	if m == nil {
		return
	}
	if m.ref.Add(-1) == 0 {
		if m.parent != nil {
			// Child ref: cascade to parent, then recycle self.
			m.parent.Release()
			m.parent = nil
			m.expiresAt = 0
			m.offset = 0
			msgRefPool.Put(m)
			return
		}
		// Top-level ref: return buffer to pool.
		if m.buf != nil {
			protocol.ReleaseBuffer(m.buf)
			m.buf = nil
		}
		m.expiresAt = 0
		m.offset = 0
		msgRefPool.Put(m)
	}
}

// Parent returns the parent MessageRef, or nil if this is a top-level ref.
func (m *MessageRef) Parent() *MessageRef {
	if m == nil {
		return nil
	}
	return m.parent
}

// Buf returns the underlying frame buffer byte slice.
// For top-level refs, returns the pooled buffer. For child refs (batch sub-messages),
// returns the zero-copy sub-slice pointing into the parent's buffer.
func (m *MessageRef) Buf() []byte {
	if m == nil {
		return nil
	}
	if m.frame != nil {
		return m.frame
	}
	if m.buf == nil {
		return nil
	}
	return *m.buf
}
