package protocol

import "testing"

func FuzzParseFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, HeaderSize))
	f.Add(make([]byte, 256))
	f.Add([]byte{MagicByte, uint8(CmdPublish), 0, 0, 0, 1, 0, 0, 0, 0})
	f.Add([]byte{MagicByte, uint8(CmdSubscribe), 0, 0, 0, 0, 0, 0, 0, 5, 1, 2, 3, 4, 5})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(make([]byte, HeaderSize-1))

	// Seed with valid serialized frames.
	buf := SerializeFrame(CmdPublish, 42, []byte("seed"))
	f.Add((*buf)[:HeaderSize])
	f.Add(*buf)
	ReleaseBuffer(buf)

	oversized := SerializeFrame(CmdPublish, 1, make([]byte, 65536))
	f.Add(*oversized)
	ReleaseBuffer(oversized)

	f.Fuzz(func(t *testing.T, data []byte) {
		// ParseFrame must NEVER panic regardless of input.
		// This catches OOB reads, unsafe.Slice panics, integer overflow.
		frame, err := ParseFrame(data)
		if err != nil {
			return // expected for random data
		}

		// If parsing succeeded, validate invariants.
		if frame.PayloadLen > uint32(len(data)-HeaderSize) {
			t.Errorf("PayloadLen %d exceeds remaining buffer %d", frame.PayloadLen, len(data)-HeaderSize)
		}
		if frame.PayloadLen > 0 && len(frame.Payload) != int(frame.PayloadLen) {
			t.Errorf("Payload length mismatch: PayloadLen=%d, len(Payload)=%d", frame.PayloadLen, len(frame.Payload))
		}
		if frame.Command < CmdPublish || frame.Command > CmdAck {
			t.Errorf("invalid command: %d", frame.Command)
		}
	})
}

func FuzzSerializeFrame(f *testing.F) {
	f.Add(uint32(0), []byte{})
	f.Add(uint32(42), []byte("hello"))
	f.Add(uint32(1), make([]byte, 4096))
	f.Add(uint32(0xFFFFFFFF), []byte{0xFF})

	f.Fuzz(func(t *testing.T, streamID uint32, payload []byte) {
		buf := SerializeFrame(CmdPublish, streamID, payload)
		defer ReleaseBuffer(buf)

		// Round-trip: serialize then parse must succeed.
		frame, err := ParseFrame(*buf)
		if err != nil {
			t.Fatalf("ParseFrame failed on valid frame: %v", err)
		}
		if frame.Command != CmdPublish {
			t.Errorf("command mismatch: got %d, want CmdPublish", frame.Command)
		}
		if frame.StreamID != streamID {
			t.Errorf("streamID mismatch: got %d, want %d", frame.StreamID, streamID)
		}
		if frame.PayloadLen != uint32(len(payload)) {
			t.Errorf("PayloadLen mismatch: got %d, want %d", frame.PayloadLen, len(payload))
		}
		if string(frame.Payload) != string(payload) {
			t.Errorf("payload mismatch")
		}
	})
}
