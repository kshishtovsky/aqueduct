package protocol

import (
	"errors"
	"sync"
	"unsafe"
)

const (
	MagicByte  = 0x1F
	HeaderSize = 10
)

const (
	CmdPublish Command = 1 + iota
	CmdSubscribe
	CmdAck
)

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

type Command uint8

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

	cmd := Command(buf[1])
	if cmd < CmdPublish || cmd > CmdAck {
		return Frame{}, errors.New("unknown command")
	}

	streamID := *(*uint32)(unsafe.Pointer(&buf[2]))
	payloadLen := *(*uint32)(unsafe.Pointer(&buf[6]))

	totalLen := HeaderSize + int(payloadLen)
	if len(buf) < totalLen {
		return Frame{}, errors.New("frame truncated: payload exceeds buffer length")
	}

	payload := unsafe.Slice(&buf[HeaderSize], payloadLen)

	return Frame{
		Command:    cmd,
		StreamID:   streamID,
		PayloadLen: payloadLen,
		Payload:    payload,
	}, nil
}

func SerializeFrame(cmd Command, streamID uint32, payload []byte) *[]byte {
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
	*(*uint32)(unsafe.Pointer(&b[2])) = streamID
	*(*uint32)(unsafe.Pointer(&b[6])) = payloadLen

	if payloadLen > 0 {
		copy(b[HeaderSize:], payload)
	}

	return bp
}

func ReleaseBuffer(bp *[]byte) {
	if bp == nil {
		return
	}
	bufPool.Put(bp)
}

func FrameSize(payloadLen uint32) int {
	return HeaderSize + int(payloadLen)
}
