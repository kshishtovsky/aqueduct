package protocol

import (
	"testing"
	"unsafe"
)

func TestParseFrame_Valid(t *testing.T) {
	payload := []byte("hello world")
	buf := SerializeFrame(CmdPublish, 42, payload)
	defer ReleaseBuffer(buf)

	frame, err := ParseFrame(*buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if frame.Command != CmdPublish {
		t.Errorf("expected CmdPublish, got %d", frame.Command)
	}
	if frame.StreamID != 42 {
		t.Errorf("expected StreamID 42, got %d", frame.StreamID)
	}
	if frame.PayloadLen != uint32(len(payload)) {
		t.Errorf("expected PayloadLen %d, got %d", len(payload), frame.PayloadLen)
	}
	if string(frame.Payload) != "hello world" {
		t.Errorf("expected payload 'hello world', got %q", frame.Payload)
	}
}

func TestParseFrame_InvalidMagicByte(t *testing.T) {
	buf := make([]byte, HeaderSize+5)
	buf[0] = 0x00

	_, err := ParseFrame(buf)
	if err == nil {
		t.Fatal("expected error for invalid magic byte, got nil")
	}
}

func TestParseFrame_TooShort(t *testing.T) {
	buf := make([]byte, HeaderSize-1)

	_, err := ParseFrame(buf)
	if err == nil {
		t.Fatal("expected error for short frame, got nil")
	}
}

func TestParseFrame_TruncatedPayload(t *testing.T) {
	buf := make([]byte, HeaderSize+5)
	buf[0] = MagicByte
	*(*uint32)(unsafe.Pointer(&buf[6])) = 100

	_, err := ParseFrame(buf)
	if err == nil {
		t.Fatal("expected error for truncated payload, got nil")
	}
}

func TestParseFrame_UnknownCommand(t *testing.T) {
	buf := make([]byte, HeaderSize)
	buf[0] = MagicByte
	buf[1] = 0xFF

	_, err := ParseFrame(buf)
	if err == nil {
		t.Fatal("expected error for unknown command, got nil")
	}
}

func TestSerializeParseRoundTrip(t *testing.T) {
	payload := []byte("round-trip test payload")
	buf := SerializeFrame(CmdSubscribe, 7, payload)
	defer ReleaseBuffer(buf)

	frame, err := ParseFrame(*buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if frame.Command != CmdSubscribe {
		t.Errorf("expected CmdSubscribe, got %d", frame.Command)
	}
	if frame.StreamID != 7 {
		t.Errorf("expected StreamID 7, got %d", frame.StreamID)
	}
	if frame.PayloadLen != uint32(len(payload)) {
		t.Errorf("expected PayloadLen %d, got %d", len(payload), frame.PayloadLen)
	}
	if string(frame.Payload) != string(payload) {
		t.Errorf("payload mismatch: expected %q, got %q", payload, frame.Payload)
	}
}

func TestFrameSize(t *testing.T) {
	size := FrameSize(0)
	if size != HeaderSize {
		t.Errorf("expected size %d for empty payload, got %d", HeaderSize, size)
	}

	size = FrameSize(256)
	if size != HeaderSize+256 {
		t.Errorf("expected size %d for 256-byte payload, got %d", HeaderSize+256, size)
	}
}

func TestFrame_Size(t *testing.T) {
	frame := Frame{
		Command:    CmdAck,
		StreamID:   1,
		PayloadLen: 5,
	}
	if frame.Size() != HeaderSize+5 {
		t.Errorf("expected size %d, got %d", HeaderSize+5, frame.Size())
	}
}

func BenchmarkParseFrame(b *testing.B) {
	payload := make([]byte, 128)
	buf := SerializeFrame(CmdPublish, 1, payload)
	defer ReleaseBuffer(buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(*buf)
	}
}

func TestParseBatchFrames(t *testing.T) {
	f1 := SerializeFrame(CmdPublish, 1, []byte("hello"))
	f2 := SerializeFrame(CmdPublish, 2, []byte("world"))
	defer ReleaseBuffer(f1)
	defer ReleaseBuffer(f2)

	batch := make([]byte, 0, len(*f1)+len(*f2))
	batch = append(batch, *f1...)
	batch = append(batch, *f2...)

	// Verify zero-copy: parsed frame slices must point into batch
	var frames [][]byte
	err := ParseBatch(batch, func(frame []byte) error {
		frames = append(frames, frame)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}

	for i, f := range frames {
		// SAFETY: &f[0] must be >= &batch[0] and <= &batch[len(batch)-1]
		fPtr := uintptr(unsafe.Pointer(&f[0]))
		batchStart := uintptr(unsafe.Pointer(&batch[0]))
		batchEnd := batchStart + uintptr(len(batch))
		if fPtr < batchStart || fPtr+uintptr(len(f)) > batchEnd {
			t.Errorf("frame %d: slice %p does not point into batch [%p, %p]", i, &f[0], &batch[0], &batch[len(batch)-1])
		}
	}

	// Validate parsed Frame structs
	for i, raw := range frames {
		frame, err := ParseFrame(raw)
		if err != nil {
			t.Fatalf("ParseFrame on batch frame %d: %v", i, err)
		}
		if frame.StreamID != uint32(i+1) {
			t.Errorf("frame %d: expected StreamID %d, got %d", i, i+1, frame.StreamID)
		}
	}
}

func TestParseBatchFrame_Truncated(t *testing.T) {
	// Single header-only frame in batch
	f := SerializeFrame(CmdPublish, 1, []byte("hello"))
	defer ReleaseBuffer(f)

	truncated := (*f)[:HeaderSize+2]
	err := ParseBatch(truncated, func(frame []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for truncated batch frame")
	}
}

func BenchmarkSerializeFrame(b *testing.B) {
	payload := make([]byte, 128)

	warmup := SerializeFrame(CmdPublish, 1, payload)
	ReleaseBuffer(warmup)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := SerializeFrame(CmdPublish, 1, payload)
		ReleaseBuffer(buf)
	}
}

func BenchmarkBatchUnpack(b *testing.B) {
	// Build a batch of 1000 frames, each with 64-byte payload
	const numFrames = 1000
	const msgSize = 64

	var frames []*[]byte
	for i := range numFrames {
		payload := make([]byte, msgSize)
		payload[0] = byte(i)
		buf := SerializeFrame(CmdPublish, uint32(i), payload)
		frames = append(frames, buf)
	}

	totalLen := 0
	for _, f := range frames {
		totalLen += len(*f)
	}
	batch := make([]byte, 0, totalLen)
	for _, f := range frames {
		batch = append(batch, *f...)
	}
	for _, f := range frames {
		ReleaseBuffer(f)
	}

	b.SetBytes(int64(totalLen))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		_ = ParseBatch(batch, func(frame []byte) error {
			count++
			return nil
		})
		if count != numFrames {
			b.Fatalf("expected %d frames, got %d", numFrames, count)
		}
	}
}

func BenchmarkBatchUnpackFrames(b *testing.B) {
	const numFrames = 1000
	const msgSize = 64

	var frames []*[]byte
	for i := range numFrames {
		payload := make([]byte, msgSize)
		payload[0] = byte(i)
		buf := SerializeFrame(CmdPublish, uint32(i), payload)
		frames = append(frames, buf)
	}

	totalLen := 0
	for _, f := range frames {
		totalLen += len(*f)
	}
	batch := make([]byte, 0, totalLen)
	for _, f := range frames {
		batch = append(batch, *f...)
	}
	for _, f := range frames {
		ReleaseBuffer(f)
	}

	b.SetBytes(int64(totalLen))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var parsed [][]byte
		parsed = parsed[:0]
		_ = ParseBatch(batch, func(frame []byte) error {
			parsed = append(parsed, frame)
			return nil
		})
		if len(parsed) != numFrames {
			b.Fatalf("expected %d frames, got %d", numFrames, len(parsed))
		}
	}
}
