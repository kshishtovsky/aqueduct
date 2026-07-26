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

// MessageRef wraps a pooled frame buffer (*[]byte) with an atomic reference counter and optional TTL expiry.
// It ensures that buffers recycled into sync.Pool are never reused while queued
// in another subscriber's pipeline or undergoing network I/O.
type MessageRef struct {
	buf       *[]byte
	ref       atomic.Int32
	expiresAt int64 // unix nano timestamp, 0 = no expiry
}

// AcquireMessageRef pulls a MessageRef from pool, wraps the frame buffer, and sets ref count to 1.
func AcquireMessageRef(buf *[]byte) *MessageRef {
	m := msgRefPool.Get().(*MessageRef)
	m.buf = buf
	m.expiresAt = 0
	m.ref.Store(1)
	return m
}

// SetExpiresAt sets the unix nanosecond expiration timestamp.
func (m *MessageRef) SetExpiresAt(exp int64) {
	if m != nil {
		m.expiresAt = exp
	}
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

// Release decrements the reference counter by 1. When the count drops to 0,
// the underlying buffer is returned to protocol.ReleaseBuffer and the MessageRef is recycled.
func (m *MessageRef) Release() {
	if m == nil {
		return
	}
	if m.ref.Add(-1) == 0 {
		if m.buf != nil {
			protocol.ReleaseBuffer(m.buf)
			m.buf = nil
		}
		m.expiresAt = 0
		msgRefPool.Put(m)
	}
}

// Buf returns the underlying frame buffer byte slice.
func (m *MessageRef) Buf() []byte {
	if m == nil || m.buf == nil {
		return nil
	}
	return *m.buf
}
