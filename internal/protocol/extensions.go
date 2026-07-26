package protocol

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	// ExtTraceContext is the TLV type for W3C Trace Context propagation.
	ExtTraceContext ExtensionType = 0x01

	// ExtTraceContextLen is the fixed value length of a Trace Context TLV:
	// TraceID (16) + SpanID (8) + TraceFlags (1) = 25 bytes.
	ExtTraceContextLen = 25

	// ExtHeaderLen is the 2-byte ExtTotalLen prefix before TLV entries.
	ExtHeaderLen = 2

	// ExtEntryHeader is the TLV entry overhead: Type (1) + Len (1) = 2 bytes.
	ExtEntryHeader = 2

	// MaxExtTotalLen limits the total TLV block size to prevent DoS.
	MaxExtTotalLen = 1024
)

// ExtensionType identifies a TLV extension in the extension block.
type ExtensionType uint8

var (
	ErrExtTooShort       = errors.New("extension block truncated")
	ErrExtTooLong        = errors.New("extension block exceeds max size")
	ErrExtEntryTruncated = errors.New("TLV entry truncated")
	ErrExtUnknownType    = errors.New("unknown TLV type, skipping")
)

// ExtTotalLen returns the total TLV block byte count from buf at offset.
// SAFETY: caller must ensure len(buf) >= offset+2.
func ExtTotalLen(buf []byte, offset int) int {
	return int(binary.LittleEndian.Uint16(buf[offset : offset+2]))
}

// SetExtTotalLen writes the total TLV block length at buf[offset:offset+2].
func SetExtTotalLen(buf []byte, offset int, n int) {
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(n))
}

// ExtBlockSize returns the full wire size of the extension region:
// 2-byte length prefix + totalLen bytes of TLV entries.
func ExtBlockSize(totalLen int) int {
	return ExtHeaderLen + totalLen
}

// ParseTLVEntry parses a single TLV entry from buf at offset.
// Returns type, value slice (pointing into buf), and the next offset.
// SAFETY: caller must bounds-check before calling.
func ParseTLVEntry(buf []byte, offset int) (typ ExtensionType, value []byte, next int, err error) {
	if offset+ExtEntryHeader > len(buf) {
		return 0, nil, offset, ErrExtEntryTruncated
	}
	typ = ExtensionType(buf[offset])
	valLen := int(buf[offset+1])
	valStart := offset + ExtEntryHeader
	if valStart+valLen > len(buf) {
		return 0, nil, offset, ErrExtEntryTruncated
	}
	if valLen == 0 {
		return typ, nil, valStart, nil
	}
	// SAFE: valStart+valLen bounds-checked above.
	value = unsafe.Slice(&buf[valStart], valLen)
	return typ, value, valStart + valLen, nil
}

// FindExtension scans the extension block at extBlock (which includes the
// 2-byte ExtTotalLen prefix) and returns the value for the given TLV type.
// Returns nil, false if type is not found. Zero-alloc.
func FindExtension(extBlock []byte, typ ExtensionType) ([]byte, bool) {
	if len(extBlock) < ExtHeaderLen {
		return nil, false
	}
	totalLen := ExtTotalLen(extBlock, 0)
	if totalLen <= 0 {
		return nil, false
	}
	if len(extBlock) < ExtHeaderLen+totalLen {
		return nil, false
	}

	offset := ExtHeaderLen
	end := ExtHeaderLen + totalLen
	for offset < end {
		if offset+ExtEntryHeader > end {
			return nil, false
		}
		t := ExtensionType(extBlock[offset])
		l := int(extBlock[offset+1])
		valStart := offset + ExtEntryHeader
		if l == 0 {
			offset = valStart
			continue
		}
		if valStart+l > end {
			return nil, false
		}
		if t == typ {
			return unsafe.Slice(&extBlock[valStart], l), true
		}
		offset = valStart + l
	}
	return nil, false
}

// ExtractTraceContext extracts a W3C Trace Context from extBlock.
// Returns TraceID (16 bytes), SpanID (8 bytes), TraceFlags (1 byte), and ok.
// All byte slices point into extBlock — zero-copy, zero-alloc.
func ExtractTraceContext(extBlock []byte) (traceID []byte, spanID []byte, traceFlags byte, ok bool) {
	val, found := FindExtension(extBlock, ExtTraceContext)
	if !found || len(val) < ExtTraceContextLen {
		return nil, nil, 0, false
	}
	// SAFE: len(val) >= ExtTraceContextLen checked above.
	return val[:16], val[16:24], val[24], true
}

// SetTraceContext writes a W3C Trace Context into buf starting at the given
// TLV entry offset. buf must have room for ExtEntryHeader + ExtTraceContextLen.
// Returns the offset after the entry.
func SetTraceContext(buf []byte, offset int, traceID []byte, spanID []byte, traceFlags byte) int {
	buf[offset] = byte(ExtTraceContext)
	buf[offset+1] = ExtTraceContextLen
	copy(buf[offset+2:offset+2+16], traceID[:16])
	copy(buf[offset+18:offset+18+8], spanID[:8])
	buf[offset+26] = traceFlags
	return offset + ExtEntryHeader + ExtTraceContextLen
}

// BuildExtensions creates a byte slice containing the full extension block
// (2-byte total len + TLV entries) for the given trace context.
// Uses slab allocator for zero-alloc on hot path.
func BuildExtensions(traceID []byte, spanID []byte, traceFlags byte) []byte {
	totalLen := ExtEntryHeader + ExtTraceContextLen
	totalSize := ExtHeaderLen + totalLen

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, totalLen)
	SetTraceContext(b, ExtHeaderLen, traceID, spanID, traceFlags)
	return b
}

// ReleaseExtensions releases an extension block allocated via BuildExtensions.
func ReleaseExtensions(b []byte) {
	if len(b) == 0 {
		return
	}
	c := cap(b)
	for _, s := range slabClasses {
		if c == s {
			globalSlab.Release(b)
			return
		}
	}
}
