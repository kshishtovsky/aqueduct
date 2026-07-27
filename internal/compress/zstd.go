package compress

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/klauspost/compress/zstd"
	"github.com/kshishtovsky/aqueduct/internal/mem"
)

const (
	AlgoZSTD uint8 = 1

	CompressionTLVAlgoOffset = 0
	CompressionTLVSizeOffset = 1
	CompressionTLVValueLen   = 5
)

var (
	ErrCorruptedPayload = errors.New("compressed payload corrupted")
)

type ZstdEngine struct {
	encPool sync.Pool
	decPool sync.Pool
	slab    *mem.SlabAllocator
}

func NewZstdEngine(slab *mem.SlabAllocator) *ZstdEngine {
	e := &ZstdEngine{
		slab: slab,
	}
	e.encPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderConcurrency(1),
				zstd.WithWindowSize(1<<16),
				zstd.WithEncoderLevel(zstd.SpeedDefault),
			)
			if err != nil {
				panic(err)
			}
			return enc
		},
	}
	e.decPool = sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
			)
			if err != nil {
				panic(err)
			}
			return dec
		},
	}
	return e
}

func (e *ZstdEngine) getEncoder() *zstd.Encoder {
	return e.encPool.Get().(*zstd.Encoder)
}

func (e *ZstdEngine) putEncoder(enc *zstd.Encoder) {
	enc.Reset(nil)
	e.encPool.Put(enc)
}

func (e *ZstdEngine) getDecoder() *zstd.Decoder {
	return e.decPool.Get().(*zstd.Decoder)
}

func (e *ZstdEngine) putDecoder(dec *zstd.Decoder) {
	_ = dec.Reset(nil)
	e.decPool.Put(dec)
}

func sameBacking(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	// #nosec G103 -- unsafe.SliceData is used to compare backing array pointers (no GC escape, no read).
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

// Compress compresses src and returns a slab-allocated buffer.
// Caller MUST call ReleaseCompressed when done.
func (e *ZstdEngine) Compress(src []byte) ([]byte, error) {
	return e.compress(src, nil)
}

func (e *ZstdEngine) compress(src []byte, reuse []byte) ([]byte, error) {
	enc := e.getEncoder()
	defer e.putEncoder(enc)

	maxSize := enc.MaxEncodedSize(len(src))
	if maxSize <= 0 {
		maxSize = len(src) + 4096
	}

	var dst []byte
	if reuse != nil && cap(reuse) >= maxSize {
		dst = reuse[:0]
	} else {
		b, err := e.slab.Acquire(maxSize)
		if err != nil {
			dst = make([]byte, 0, maxSize)
		} else {
			dst = b[:0]
		}
	}

	capBefore := cap(dst)
	result := enc.EncodeAll(src, dst)

	if sameBacking(result, dst) || cap(result) == capBefore {
		return result, nil
	}

	if cap(dst) == capBefore && cap(dst) > 0 {
		e.slab.Release(dst[:cap(dst)])
	}
	return result, nil
}

func (e *ZstdEngine) Decompress(src []byte, uncompressedSize int) ([]byte, error) {
	return e.decompress(src, uncompressedSize, nil)
}

func (e *ZstdEngine) decompress(src []byte, uncompressedSize int, reuse []byte) ([]byte, error) {
	dec := e.getDecoder()
	defer e.putDecoder(dec)

	// Use make allocation for the output buffer because the decompressed data
	// may be referenced asynchronously (e.g., via MessageRef sub-frames).
	// Slab-allocated buffers cannot be safely released while child references exist.
	var dst []byte
	if reuse != nil && cap(reuse) >= uncompressedSize {
		dst = reuse[:0]
	} else {
		dst = make([]byte, 0, uncompressedSize)
	}

	result, err := dec.DecodeAll(src, dst)
	if err != nil {
		return nil, err
	}

	if cap(result) >= uncompressedSize {
		return result[:uncompressedSize], nil
	}
	return result, nil
}

// ReleaseBuf returns a buffer to the slab allocator.
func (e *ZstdEngine) ReleaseBuf(buf []byte) {
	if len(buf) == 0 {
		return
	}
	c := cap(buf)
	for _, s := range slabClasses {
		if c == s {
			e.slab.Release(buf)
			return
		}
	}
}

var slabClasses = []int{128, 256, 512, 2048, 8192, 32768}

// CompressedSize returns an upper bound on compressed output size.
func CompressedSize(srcLen int) int {
	// MaxEncodedSize requires an encoder instance, so use a safe heuristic.
	if srcLen <= 0 {
		return 0
	}
	// ZSTD worst case: input + 1/255 overhead + 16 bytes frame header.
	extra := srcLen / 255
	if extra < 16 {
		extra = 16
	}
	return srcLen + extra + 16
}
