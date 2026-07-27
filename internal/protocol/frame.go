package protocol

import (
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"

	"github.com/kshishtovsky/aqueduct/internal/mem"
)

const (
	MagicByte  = 0x1F
	HeaderSize = 10
	// MeshForwardedBit is set in the Command byte (bit 7) to mark frames that have
	// already been forwarded by a peer node. Receivers must NOT re-forward such frames,
	// preventing mesh broadcast storms. The lower 6 bits remain the command opcode.
	MeshForwardedBit Command = 0x80
	// HasExtensionsBit is set in the Command byte (bit 6) to mark frames that
	// carry a TLV extension block between the header and the payload.
	// When set, the wire format becomes:
	//   [Magic:1][Cmd:1][StreamID:4][DataLen:4][ExtTotalLen:2][TLV...][Payload: ...]
	// where DataLen covers everything from offset 10 (ExtTotalLen) to the end of the frame.
	// Old parsers that mask only MeshForwardedBit (0x80) will skip the frame as
	// unknown opcode but correctly advance past DataLen bytes, preserving wire alignment.
	HasExtensionsBit Command = 0x40
)

const (
	CmdPublish Command = 1 + iota
	CmdSubscribe
	CmdUnsubscribe
	CmdAck
	CmdPublishBatch
	CmdNack
)

type Command uint8

var (
	globalSlab = mem.New()

	bufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 1024)
			return &b
		},
	}
)

func SetSlabAllocator(sa *mem.SlabAllocator) {
	if sa != nil {
		globalSlab = sa
	}
}

type Frame struct {
	Command    Command
	StreamID   uint32
	PayloadLen uint32
	Payload    []byte
	Extensions []byte // raw TLV block (2-byte ExtTotalLen + entries), nil if none
}

// DataLen returns the total byte count from offset 10 (after header) to the
// end of the frame: ExtBlockSize + PayloadLen. This covers the entire wire
// data segment for both old (no extensions) and new frames.
func (f Frame) DataLen() uint32 {
	if len(f.Extensions) > 0 {
		// #nosec G115 -- Extensions size is bounded by MaxExtTotalLen (1024) and payload by MaxFrameSize, so len(f.Extensions) + f.PayloadLen < 2^32.
		return uint32(len(f.Extensions)) + f.PayloadLen
	}
	return f.PayloadLen
}

func (f Frame) Size() int {
	return HeaderSize + int(f.DataLen())
}

// HasExtensions reports whether the frame carries a TLV extension block.
func (f Frame) HasExtensions() bool {
	return len(f.Extensions) > 0
}

func ParseFrame(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, errors.New("frame too short for header")
	}

	if buf[0] != MagicByte {
		return Frame{}, errors.New("invalid magic byte")
	}

	// Preserve the raw command byte; strip both control bits for opcode validation.
	rawCmd := Command(buf[1])
	cmd := rawCmd & (^MeshForwardedBit) & (^HasExtensionsBit)
	if cmd < CmdPublish || cmd > CmdNack {
		return Frame{}, errors.New("unknown command")
	}

	streamID := binary.LittleEndian.Uint32(buf[2:6])
	dataLen := binary.LittleEndian.Uint32(buf[6:10])

	totalLen := HeaderSize + int(dataLen)
	if len(buf) < totalLen {
		return Frame{}, errors.New("frame truncated: data exceeds buffer length")
	}

	// Parse extension block if present
	var extBlock []byte
	var payloadStart int
	var payloadLen uint32

	if rawCmd&HasExtensionsBit != 0 {
		if int(dataLen) < ExtHeaderLen {
			return Frame{}, errors.New("frame too short for extension header")
		}
		extTotal := ExtTotalLen(buf, HeaderSize)
		if extTotal > MaxExtTotalLen {
			return Frame{}, errors.New("extension block exceeds max size")
		}
		extBlockEnd := HeaderSize + ExtBlockSize(extTotal)
		if extBlockEnd > HeaderSize+int(dataLen) {
			return Frame{}, errors.New("extensions exceed declared data length")
		}
		// SAFE: bounds verified above.
		// #nosec G103 -- unsafe.Slice is used to construct a zero-copy view of an already-bounds-checked sub-slice of buf (extBlockEnd <= HeaderSize+int(dataLen) verified above).
		extBlock = unsafe.Slice(&buf[HeaderSize], ExtBlockSize(extTotal))
		payloadStart = extBlockEnd
		// #nosec G115 -- both operands are bounded by MaxFrameSize (uint32) so the subtraction is safe.
		payloadLen = dataLen - uint32(ExtBlockSize(extTotal))
	} else {
		payloadStart = HeaderSize
		payloadLen = dataLen
	}

	// Zero-length payloads: return nil slice without calling unsafe.Slice,
	// which would require buf[payloadStart] to be a valid address even when
	// payloadStart equals the buffer length exactly.
	if payloadLen == 0 {
		return Frame{
			Command:    rawCmd, // preserve control bits for callers
			StreamID:   streamID,
			PayloadLen: 0,
			Payload:    nil,
			Extensions: extBlock,
		}, nil
	}

	// SAFE: We verified len(buf) >= headerSize + int(dataLen) above,
	// and payloadStart + payloadLen <= headerSize + dataLen.
	// #nosec G103 -- unsafe.Slice is used to construct a zero-copy view of an already-bounds-checked sub-slice of buf (payloadStart + payloadLen <= HeaderSize+int(dataLen) verified above).
	payload := unsafe.Slice(&buf[payloadStart], payloadLen)

	return Frame{
		Command:    rawCmd, // preserve control bits for callers
		StreamID:   streamID,
		PayloadLen: payloadLen,
		Payload:    payload,
		Extensions: extBlock,
	}, nil
}

// IsForwarded reports whether the MeshForwarded flag is set on a frame command byte.
func IsForwarded(cmd Command) bool {
	return cmd&MeshForwardedBit != 0
}

// SetForwarded returns the command byte with the MeshForwarded flag set.
func SetForwarded(cmd Command) Command {
	return cmd | MeshForwardedBit
}

// OpcodeOf strips both control bits (MeshForwarded and HasExtensions) and returns
// the bare opcode.
func OpcodeOf(cmd Command) Command {
	return cmd & ^MeshForwardedBit & ^HasExtensionsBit
}

var (
	slabClasses = []int{128, 256, 512, 2048, 8192, 32768}
	bufPtrPool  = sync.Pool{
		New: func() any {
			var b []byte
			return &b
		},
	}
)

func serializeFrameSlab(cmd Command, streamID uint32, payload []byte) *[]byte {
	// #nosec G115 -- payload length is bounded by MaxFrameSize (set at the transport layer to 64KB default), so len(payload) < 2^32.
	payloadLen := uint32(len(payload))
	totalSize := HeaderSize + int(payloadLen)

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		return serializeFramePool(cmd, streamID, payload)
	}

	b = b[:totalSize]
	b[0] = MagicByte
	b[1] = uint8(cmd)
	binary.LittleEndian.PutUint32(b[2:6], streamID)
	binary.LittleEndian.PutUint32(b[6:10], payloadLen)
	if payloadLen > 0 {
		copy(b[HeaderSize:], payload)
	}

	bp := bufPtrPool.Get().(*[]byte)
	*bp = b
	return bp
}

func serializeFramePool(cmd Command, streamID uint32, payload []byte) *[]byte {
	// #nosec G115 -- payload length is bounded by MaxFrameSize.
	payloadLen := uint32(len(payload))
	totalSize := HeaderSize + int(payloadLen)

	raw := bufPool.Get()
	var bp *[]byte
	if raw == nil {
		b := make([]byte, totalSize)
		bp = &b
	} else {
		bp = raw.(*[]byte)
		b := *bp
		if cap(b) < totalSize {
			*bp = make([]byte, totalSize)
		} else {
			*bp = b[:totalSize]
		}
	}

	b := *bp
	b[0] = MagicByte
	b[1] = uint8(cmd)
	binary.LittleEndian.PutUint32(b[2:6], streamID)
	binary.LittleEndian.PutUint32(b[6:10], payloadLen)

	if payloadLen > 0 {
		copy(b[HeaderSize:], payload)
	}

	return bp
}

func SerializeFrame(cmd Command, streamID uint32, payload []byte) *[]byte {
	return serializeFrameSlab(cmd, streamID, payload)
}

func serializeFrameWithExtensionsSlab(cmd Command, streamID uint32, extensions []byte, payload []byte) *[]byte {
	// #nosec G115 -- payload length is bounded by MaxFrameSize.
	payloadLen := uint32(len(payload))
	extLen := len(extensions)
	totalSize := HeaderSize + extLen + int(payloadLen)

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	// #nosec G115 -- extLen <= MaxExtTotalLen (1024), payloadLen <= MaxFrameSize; sum < 2^32.
	dataLen := uint32(extLen) + payloadLen
	b[0] = MagicByte
	b[1] = uint8(cmd | HasExtensionsBit)
	binary.LittleEndian.PutUint32(b[2:6], streamID)
	binary.LittleEndian.PutUint32(b[6:10], dataLen)
	if extLen > 0 {
		copy(b[HeaderSize:], extensions)
	}
	if payloadLen > 0 {
		copy(b[HeaderSize+extLen:], payload)
	}

	bp := bufPtrPool.Get().(*[]byte)
	*bp = b
	return bp
}

// SerializeFrameWithExtensions serializes a frame with a TLV extension block.
// Sets the HasExtensionsBit in the command byte and writes the extension
// block between the header and the payload. The extensions parameter should
// include the 2-byte ExtTotalLen prefix (see BuildExtensions).
func SerializeFrameWithExtensions(cmd Command, streamID uint32, extensions []byte, payload []byte) *[]byte {
	return serializeFrameWithExtensionsSlab(cmd, streamID, extensions, payload)
}

func ReleaseBuffer(bp *[]byte) {
	if bp == nil {
		return
	}
	b := *bp
	c := cap(b)
	for _, s := range slabClasses {
		if c == s {
			globalSlab.Release(b)
			*bp = nil
			bufPtrPool.Put(bp)
			return
		}
	}
	bufPool.Put(bp)
}

// PayloadLen extracts the payload length from a frame buffer at offset 6.
// SAFETY: Caller must ensure len(buf) >= HeaderSize (10).
func PayloadLen(buf []byte) uint32 {
	if len(buf) < HeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint32(buf[6:10])
}

func FrameSize(payloadLen uint32) int {
	return HeaderSize + int(payloadLen)
}

// ParseBatchFrame extracts the next complete frame slice from buf starting at offset.
// The returned frame slice includes the header, any extension block, and the payload.
// SAFE: All bounds checks are performed before returning the sub-slice.
func ParseBatchFrame(buf []byte, offset int) (frame []byte, nextOffset int, err error) {
	remaining := buf[offset:]
	if len(remaining) < HeaderSize {
		return nil, offset, errors.New("batch frame too short: header truncated")
	}
	if remaining[0] != MagicByte {
		return nil, offset, errors.New("invalid magic byte in batch frame")
	}
	cmd := Command(remaining[1])
	opcode := OpcodeOf(cmd)
	if opcode < CmdPublish || opcode > CmdNack {
		return nil, offset, errors.New("unknown command in batch frame")
	}
	dataLen := binary.LittleEndian.Uint32(remaining[6:10])
	totalLen := HeaderSize + int(dataLen)
	if len(remaining) < totalLen {
		return nil, offset, errors.New("batch frame truncated: data exceeds remaining buffer")
	}
	return buf[offset : offset+totalLen], offset + totalLen, nil
}

// ParseBatch iterates over a batch payload and calls fn for each complete frame.
// The frame slice passed to fn points into the original batch buffer (zero-copy).
func ParseBatch(batchBuf []byte, fn func(frame []byte) error) error {
	offset := 0
	for offset < len(batchBuf) {
		frame, nextOffset, err := ParseBatchFrame(batchBuf, offset)
		if err != nil {
			return err
		}
		if err := fn(frame); err != nil {
			return err
		}
		offset = nextOffset
	}
	return nil
}
