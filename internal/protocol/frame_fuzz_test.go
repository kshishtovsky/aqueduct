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
		if opcode := OpcodeOf(frame.Command); opcode < CmdPublish || opcode > CmdNack {
			t.Errorf("invalid command opcode: raw=%d opcode=%d", frame.Command, opcode)
		}
	})
}

func FuzzParseBatch(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, HeaderSize))
	f.Add(make([]byte, 256))

	// Build a valid batch of 3 frames
	f1 := SerializeFrame(CmdPublish, 1, []byte("msg1"))
	f2 := SerializeFrame(CmdPublish, 2, []byte("msg2"))
	f3 := SerializeFrame(CmdPublish, 3, []byte("msg3"))
	batch := make([]byte, 0, len(*f1)+len(*f2)+len(*f3))
	batch = append(batch, *f1...)
	batch = append(batch, *f2...)
	batch = append(batch, *f3...)
	ReleaseBuffer(f1)
	ReleaseBuffer(f2)
	ReleaseBuffer(f3)
	f.Add(batch)
	f.Add(batch[:HeaderSize+5]) // truncated

	f.Fuzz(func(t *testing.T, data []byte) {
		// ParseBatch must NEVER panic regardless of input.
		frameCount := 0
		err := ParseBatch(data, func(frame []byte) error {
			if len(frame) < HeaderSize {
				t.Errorf("frame slice shorter than header: %d", len(frame))
			}
			if frame[0] != MagicByte {
				t.Errorf("invalid magic byte in frame %d", frameCount)
			}
			frameCount++
			return nil
		})
		if err != nil {
			return // expected for random data
		}
		if frameCount == 0 && len(data) > 0 {
			t.Errorf("ParseBatch returned nil error but parsed 0 frames for %d bytes", len(data))
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

// FuzzExtractProducerID ensures the zero-copy extractor never panics on
// arbitrary TLV input. Unknown TLV types are silently skipped.
func FuzzExtractProducerID(f *testing.F) {
	// Valid 8-byte block.
	f.Add([]byte{0, 0, byte(ExtProducerID), ExtProducerIDLen, 1, 2, 3, 4, 5, 6, 7, 8})
	// Truncated header.
	f.Add([]byte{byte(ExtProducerID)})
	// Truncated value.
	f.Add([]byte{byte(ExtProducerID), 8, 1, 2})
	// Wrong type.
	f.Add([]byte{0, 0, 0xFF, 8, 1, 2, 3, 4, 5, 6, 7, 8})

	f.Fuzz(func(t *testing.T, extBlock []byte) {
		// Must NEVER panic regardless of input.
		_, _ = ExtractProducerID(extBlock)
		_, _ = ExtractSeqNum(extBlock)
	})
}
