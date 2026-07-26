package transport

import (
	"testing"
	"unsafe"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

func TestReadBufPool_GetBuf(t *testing.T) {
	buf := _rp.GetBuf(1024)
	if cap(buf) < 1024 {
		t.Errorf("expected cap >= 1024, got %d", cap(buf))
	}
	if len(buf) != 1024 {
		t.Errorf("expected len 1024, got %d", len(buf))
	}
	_rp.PutBuf(buf)
}

func TestReadBufPool_GetBufGrow(t *testing.T) {
	buf := _rp.GetBuf(2048)
	if cap(buf) < 2048 {
		t.Errorf("expected cap >= 2048, got %d", cap(buf))
	}
	_rp.PutBuf(buf)
}

func TestReadBufPool_PutBufSmall(t *testing.T) {
	// Small buffers should not be returned to pool.
	small := make([]byte, 0, 128)
	_rp.PutBuf(small) // should not panic
}

func TestPayloadLen(t *testing.T) {
	buf := make([]byte, protocol.HeaderSize+10)
	*(*uint32)(unsafe.Pointer(&buf[6])) = 42

	got := protocol.PayloadLen(buf)
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}
