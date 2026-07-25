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
