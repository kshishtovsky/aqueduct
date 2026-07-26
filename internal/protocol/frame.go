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
	// preventing mesh broadcast storms. The lower 7 bits remain the command opcode.
	MeshForwardedBit Command = 0x80
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
}

func (f Frame) Size() int {
	return HeaderSize + int(f.PayloadLen)
}

func ParseFrame(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, errors.New("frame too short for header")
	}

	if buf[0] != MagicByte {
		return Frame{}, errors.New("invalid magic byte")
	}

	// Mask off the MeshForwarded bit before opcode validation; preserve it in the Frame.
	rawCmd := Command(buf[1])
	cmd := rawCmd & ^MeshForwardedBit
	if cmd < CmdPublish || cmd > CmdNack {
		return Frame{}, errors.New("unknown command")
	}

	streamID := binary.LittleEndian.Uint32(buf[2:6])
	payloadLen := binary.LittleEndian.Uint32(buf[6:10])

	totalLen := HeaderSize + int(payloadLen)
	if len(buf) < totalLen {
		return Frame{}, errors.New("frame truncated: payload exceeds buffer length")
	}

	// Zero-length payloads: return nil slice without calling unsafe.Slice,
	// which would require buf[HeaderSize] to be a valid address even when
	// the buffer length equals HeaderSize exactly.
	if payloadLen == 0 {
		return Frame{
			Command:    cmd,
			StreamID:   streamID,
			PayloadLen: 0,
			Payload:    nil,
		}, nil
	}

	// SAFE: We verified len(buf) >= HeaderSize + int(payloadLen) above.
	// payloadLen > 0, so &buf[HeaderSize] is within bounds, and the slice
	// length payloadLen is bounded by the remaining buffer bytes.
	payload := unsafe.Slice(&buf[HeaderSize], payloadLen)

	return Frame{
		Command:    rawCmd, // preserve MeshForwarded bit for callers
		StreamID:   streamID,
		PayloadLen: payloadLen,
		Payload:    payload,
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

// OpcodeOf strips the MeshForwarded bit and returns the bare opcode.
func OpcodeOf(cmd Command) Command {
	return cmd & ^MeshForwardedBit
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
// It returns the frame slice (pointing into the original buf), the next offset, and any error.
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
	payloadLen := binary.LittleEndian.Uint32(remaining[6:10])
	totalLen := HeaderSize + int(payloadLen)
	if len(remaining) < totalLen {
		return nil, offset, errors.New("batch frame truncated: payload exceeds remaining buffer")
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
