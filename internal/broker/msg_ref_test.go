package broker

import (
	"testing"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

func TestMessageRefCounting(t *testing.T) {
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, []byte("test"))
	msg := AcquireMessageRef(buf)

	if msg.ref.Load() != 1 {
		t.Errorf("expected initial ref 1, got %d", msg.ref.Load())
	}
	if string(msg.Buf()) == "" {
		t.Error("expected non-empty buffer")
	}

	msg.Retain()
	if msg.ref.Load() != 2 {
		t.Errorf("expected ref 2 after Retain, got %d", msg.ref.Load())
	}

	msg.Release()
	if msg.ref.Load() != 1 {
		t.Errorf("expected ref 1 after Release, got %d", msg.ref.Load())
	}
	if msg.buf == nil {
		t.Error("buffer should not be nil before final release")
	}

	msg.Release()
	if msg.buf != nil {
		t.Error("buffer should be nil after final release")
	}
}

func TestNilMessageRefSafety(t *testing.T) {
	var nilMsg *MessageRef
	nilMsg.Retain()
	nilMsg.Release()
	if nilMsg.Buf() != nil {
		t.Error("expected nil slice for nil MessageRef")
	}
}
