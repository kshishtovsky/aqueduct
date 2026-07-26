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

func TestNestedMessageRefCounting(t *testing.T) {
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, []byte("test"))
	parent := AcquireMessageRef(buf)

	// Create 5 children from the batch
	const numChildren = 5
	children := make([]*MessageRef, numChildren)
	for i := range numChildren {
		// Each child shares the parent buffer at offset 0 (simplified)
		parentBuf := parent.Buf()
		childFrame := parentBuf[protocol.HeaderSize+i : protocol.HeaderSize+i+1] // sub-slice for demo
		children[i] = AcquireChildMessageRef(parent, childFrame, uint64(i+1), 0)
	}

	// Release all children
	for _, c := range children {
		if c.Buf() == nil {
			t.Error("child buf should not be nil before release")
		}
		c.Release()
		// After release, child should still be safe (no nil deref)
	}

	// Parent should still have initial ref 1 (the one AcquireMessageRef set)
	// At this point, parent.ref should be 1 (AcquireMessageRef set it to 1,
	// AcquireChildMessageRef incremented it to 1+5=6, then all 5 children
	// released -> each calls parent.Release() -> ref goes 6->5->4->3->2->1)
	// Verify parent buffer is still alive
	if parent.Buf() == nil {
		t.Error("parent buf should not be nil until parent.Release()")
	}

	// Final parent release
	parent.Release()
	if parent.Buf() != nil {
		t.Error("parent buf should be nil after final release")
	}
}

// TestNestedRefReleaseRace verifies no data races when releasing refs concurrently.
func TestNestedRefReleaseRace(t *testing.T) {
	buf := protocol.SerializeFrame(protocol.CmdPublish, 1, []byte("race-test"))
	parent := AcquireMessageRef(buf)

	const numChildren = 100
	done := make(chan struct{}, numChildren)

	for i := range numChildren {
		child := AcquireChildMessageRef(parent, parent.Buf(), uint64(i+1), 0)
		go func(c *MessageRef) {
			c.Release()
			done <- struct{}{}
		}(child)
	}

	// Wait for all children to release
	for range numChildren {
		<-done
	}

	// Parent now has ref = 1. Release it.
	parent.Release()
}

// TestBatchRefLifecycle verifies the full lifecycle of a batch parent MessageRef
// with children: create batch, create children, release children, release parent.
// At the end the batch buffer must have been returned to sync.Pool.
func TestBatchRefLifecycle(t *testing.T) {
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, []byte("batch-data"))
	parent := AcquireMessageRef(buf)

	const numMsgs = 10
	children := make([]*MessageRef, numMsgs)
	for i := range numMsgs {
		parentBuf := parent.Buf()
		sliceStart := protocol.HeaderSize + i
		if sliceStart >= len(parentBuf) {
			sliceStart = len(parentBuf) - 1
		}
		frame := parentBuf[sliceStart : sliceStart+1]
		children[i] = AcquireChildMessageRef(parent, frame, uint64(i+1), 0)
	}

	// Release a few children at a time, simulating slow delivery
	for i := 0; i < numMsgs/2; i++ {
		children[i].Release()
		children[i] = nil
	}

	// parent ref still > 1, buf must be valid
	if len(parent.Buf()) == 0 {
		t.Error("parent buffer should still be alive after partial child release")
	}

	// Release remaining children
	for i := numMsgs / 2; i < numMsgs; i++ {
		if children[i] != nil {
			children[i].Release()
			children[i] = nil
		}
	}

	// All children released. Parent ref should be 1 (from AcquireMessageRef).
	// Parent buffer must still be valid until parent.Release()
	if len(parent.Buf()) == 0 {
		t.Error("parent buffer should be alive until parent.Release()")
	}

	parent.Release()
	// After parent release, buf should be nil
	if parent.Buf() != nil {
		t.Error("parent buf should be nil after final release")
	}
}

// TestBatchRefWithDrops simulates dropping messages via backpressure and
// verifies the batch buffer is eventually released.
func TestBatchRefWithDrops(t *testing.T) {
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, make([]byte, 100))
	parent := AcquireMessageRef(buf)

	const numChildren = 20
	for i := range numChildren {
		ch := AcquireChildMessageRef(parent, parent.Buf(), uint64(i+1), 0)
		// Simulate PolicyDropNewest: release immediately (drop)
		ch.Release()
	}

	// All children dropped. Parent ref should be 1.
	if len(parent.Buf()) == 0 {
		t.Error("parent buffer should be alive after all children dropped")
	}

	parent.Release()
}

func TestNilMessageRefSafety(t *testing.T) {
	var nilMsg *MessageRef
	nilMsg.Retain()
	nilMsg.Release()
	if nilMsg.Buf() != nil {
		t.Error("expected nil slice for nil MessageRef")
	}
}
