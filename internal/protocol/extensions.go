package protocol

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	// ExtTraceContext is the TLV type for W3C Trace Context propagation.
	ExtTraceContext ExtensionType = 0x01

	// ExtCompression is the TLV type for payload compression metadata.
	// Value: [Algo:1][UncompressedSize:4] (5 bytes total).
	// Algo 1 = ZSTD. UncompressedSize is little-endian uint32.
	ExtCompression ExtensionType = 0x02

	// ExtCompressionValueLen is the fixed value length of a Compression TLV:
	// Algo (1) + UncompressedSize (4) = 5 bytes.
	ExtCompressionValueLen = 5

	// AlgoZSTD is the algorithm identifier for ZSTD compression.
	AlgoZSTD uint8 = 1

	// ExtTraceContextLen is the fixed value length of a Trace Context TLV:
	// TraceID (16) + SpanID (8) + TraceFlags (1) = 25 bytes.
	ExtTraceContextLen = 25

	// ExtRetryOffset is the TLV type for NACK retry offset tracking (internal use).
	// Value: [OriginalOffset:8] — little-endian uint64.
	// When present, the delivery loop uses this offset instead of the topic counter
	// for frame cache key and wire offset, ensuring NACK counters correctly converge
	// to max_retries and trigger DLQ.
	ExtRetryOffset ExtensionType = 0xF0

	// ExtHeaderLen is the 2-byte ExtTotalLen prefix before TLV entries.
	ExtHeaderLen = 2

	// ExtEntryHeader is the TLV entry overhead: Type (1) + Len (1) = 2 bytes.
	ExtEntryHeader = 2

	// ExtPriority is the TLV type for message priority QoS.
	// Value: [PriorityLevel: 1] (1 byte).
	// Levels: 0 (Highest), 1 (High), 2 (Normal), 3 (Low). Default: 2 (Normal).
	ExtPriority ExtensionType = 0x03

	// ExtPriorityLen is the fixed value length of a Priority TLV: 1 byte.
	ExtPriorityLen = 1

	// Message priority levels.
	PriorityHighest uint8 = 0
	PriorityHigh    uint8 = 1
	PriorityNormal  uint8 = 2
	PriorityLow     uint8 = 3
	DefaultPriority uint8 = PriorityNormal

	// ExtProducerID is the TLV type for the Idempotent Producer identifier.
	// Value: [ID: 8] — little-endian uint64 producer ID.
	// Producers configured with idempotent semantics include this TLV in
	// every Publish frame. The broker uses it to partition sequence numbers
	// and prevents duplicate delivery on producer retry.
	ExtProducerID ExtensionType = 0x04

	// ExtProducerIDLen is the fixed value length of a ProducerID TLV: 8 bytes.
	ExtProducerIDLen = 8

	// ExtSeqNum is the TLV type for the Idempotent Producer sequence number.
	// Value: [Seq: 8] — little-endian uint64 monotonically increasing per ProducerID.
	// The broker drops messages whose (ProducerID, SeqNum) pair has already
	// been observed within the sliding window. If SeqNum is far behind the
	// highest seen value, the broker treats the frame as a protocol error
	// (close stream).
	ExtSeqNum ExtensionType = 0x05

	// ExtSeqNumLen is the fixed value length of a SeqNum TLV: 8 bytes.
	ExtSeqNumLen = 8

	// IdempotentEntrySize is the total byte size of a single ProducerID+SeqNum
	// TLV pair (header + 8 bytes ID + header + 8 bytes seq).
	IdempotentEntrySize = ExtEntryHeader + ExtProducerIDLen + ExtEntryHeader + ExtSeqNumLen

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

// ExtractPriority extracts the Priority level from extBlock.
// Returns priority level (0..3). If no priority extension is present or invalid,
// returns DefaultPriority (2), false. Zero-alloc.
func ExtractPriority(extBlock []byte) (priority uint8, ok bool) {
	val, found := FindExtension(extBlock, ExtPriority)
	if !found || len(val) < ExtPriorityLen {
		return DefaultPriority, false
	}
	p := val[0]
	if p > PriorityLow {
		return DefaultPriority, false
	}
	return p, true
}

// BuildPriorityExtension creates a TLV extension block containing a single Priority entry.
// Uses slab allocator for zero-alloc on hot path.
func BuildPriorityExtension(priority uint8) []byte {
	totalLen := ExtEntryHeader + ExtPriorityLen
	totalSize := ExtHeaderLen + totalLen

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, totalLen)
	b[ExtHeaderLen] = byte(ExtPriority)
	b[ExtHeaderLen+1] = ExtPriorityLen
	b[ExtHeaderLen+2] = priority
	return b
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

// BuildCompressionExtension creates a TLV entry value for compression metadata.
// Returns a slab-allocated extension block containing a single Compression TLV entry.
// Caller must ReleaseExtensions on the result.
func BuildCompressionExtension(algo uint8, uncompressedSize uint32) []byte {
	totalSize := ExtHeaderLen + ExtEntryHeader + ExtCompressionValueLen
	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, ExtEntryHeader+ExtCompressionValueLen)
	b[ExtHeaderLen] = byte(ExtCompression)
	b[ExtHeaderLen+1] = ExtCompressionValueLen
	b[ExtHeaderLen+2] = algo
	binary.LittleEndian.PutUint32(b[ExtHeaderLen+3:ExtHeaderLen+7], uncompressedSize)
	return b
}

// BuildMergedExtensionsWithCompression returns a new extension block containing all
// entries from existing plus a Compression TLV entry. If existing is nil, returns
// only the Compression TLV block. Caller must ReleaseExtensions on the result.
func BuildMergedExtensionsWithCompression(existing []byte, uncompressedSize uint32) []byte {
	var existingEntries []byte
	if len(existing) >= ExtHeaderLen {
		totalLen := ExtTotalLen(existing, 0)
		if totalLen > 0 && len(existing) >= ExtHeaderLen+totalLen {
			existingEntries = existing[ExtHeaderLen : ExtHeaderLen+totalLen]
		}
	}

	compEntrySize := ExtEntryHeader + ExtCompressionValueLen
	totalEntries := len(existingEntries) + compEntrySize
	totalSize := ExtHeaderLen + totalEntries

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, totalEntries)
	off := ExtHeaderLen

	if len(existingEntries) > 0 {
		copy(b[off:], existingEntries)
		off += len(existingEntries)
	}

	b[off] = byte(ExtCompression)
	b[off+1] = ExtCompressionValueLen
	b[off+2] = AlgoZSTD
	binary.LittleEndian.PutUint32(b[off+3:off+7], uncompressedSize)

	return b
}

// StripExtension returns a new extension block with all entries of the given type removed.
// Returns nil if no entries remain. Uses slab allocator for the new block.
// SAFETY: extBlock is not modified.
func StripExtension(extBlock []byte, typ ExtensionType) []byte {
	if len(extBlock) < ExtHeaderLen {
		return nil
	}
	totalLen := ExtTotalLen(extBlock, 0)
	if totalLen == 0 {
		return nil
	}
	if len(extBlock) < ExtHeaderLen+totalLen {
		return nil
	}

	src := extBlock[ExtHeaderLen : ExtHeaderLen+totalLen]
	var keepBytes int
	off := 0
	for off < len(src) {
		if off+ExtEntryHeader > len(src) {
			break
		}
		t := ExtensionType(src[off])
		l := int(src[off+1])
		if t != typ {
			keepBytes += ExtEntryHeader + l
		}
		off += ExtEntryHeader + l
	}

	if keepBytes == 0 {
		return nil
	}

	blockSize := ExtHeaderLen + keepBytes
	dst, err := globalSlab.Acquire(blockSize)
	if err != nil {
		dst = make([]byte, blockSize)
	} else {
		dst = dst[:blockSize]
	}

	SetExtTotalLen(dst, 0, keepBytes)
	dstOff := ExtHeaderLen
	srcOff := 0
	for srcOff < len(src) {
		if srcOff+ExtEntryHeader > len(src) {
			break
		}
		t := ExtensionType(src[srcOff])
		l := int(src[srcOff+1])
		if t != typ {
			dst[dstOff] = byte(t)
			dst[dstOff+1] = byte(l)
			if l > 0 {
				copy(dst[dstOff+2:dstOff+2+l], src[srcOff+2:srcOff+2+l])
			}
			dstOff += ExtEntryHeader + l
		}
		srcOff += ExtEntryHeader + l
	}
	return dst[:blockSize]
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

// ExtractProducerID extracts the Idempotent Producer identifier from extBlock.
// Returns (id, true) if the TLV is present and well-formed; (0, false) otherwise.
// Zero-alloc, zero-copy.
func ExtractProducerID(extBlock []byte) (uint64, bool) {
	val, found := FindExtension(extBlock, ExtProducerID)
	if !found || len(val) < ExtProducerIDLen {
		return 0, false
	}
	return binary.LittleEndian.Uint64(val[:ExtProducerIDLen]), true
}

// ExtractSeqNum extracts the Idempotent Producer sequence number from extBlock.
// Returns (seq, true) if the TLV is present and well-formed; (0, false) otherwise.
// Zero-alloc, zero-copy.
func ExtractSeqNum(extBlock []byte) (uint64, bool) {
	val, found := FindExtension(extBlock, ExtSeqNum)
	if !found || len(val) < ExtSeqNumLen {
		return 0, false
	}
	return binary.LittleEndian.Uint64(val[:ExtSeqNumLen]), true
}

// SetProducerID writes a ProducerID TLV entry into buf starting at offset.
// buf must have room for ExtEntryHeader + ExtProducerIDLen bytes.
// Returns the offset after the entry.
func SetProducerID(buf []byte, offset int, producerID uint64) int {
	buf[offset] = byte(ExtProducerID)
	buf[offset+1] = ExtProducerIDLen
	binary.LittleEndian.PutUint64(buf[offset+2:offset+2+ExtProducerIDLen], producerID)
	return offset + ExtEntryHeader + ExtProducerIDLen
}

// SetSeqNum writes a SeqNum TLV entry into buf starting at offset.
// buf must have room for ExtEntryHeader + ExtSeqNumLen bytes.
// Returns the offset after the entry.
func SetSeqNum(buf []byte, offset int, seqNum uint64) int {
	buf[offset] = byte(ExtSeqNum)
	buf[offset+1] = ExtSeqNumLen
	binary.LittleEndian.PutUint64(buf[offset+2:offset+2+ExtSeqNumLen], seqNum)
	return offset + ExtEntryHeader + ExtSeqNumLen
}

// BuildIdempotentExtension creates a TLV extension block that carries both
// the ProducerID and SeqNum TLVs. This is the canonical block an idempotent
// producer attaches to every Publish frame. Returns a slab-allocated block
// sized for the two entries; caller must ReleaseExtensions on the result.
func BuildIdempotentExtension(producerID, seqNum uint64) []byte {
	totalLen := IdempotentEntrySize
	totalSize := ExtHeaderLen + totalLen

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, totalLen)
	off := ExtHeaderLen
	off = SetProducerID(b, off, producerID)
	off = SetSeqNum(b, off, seqNum)
	return b
}

// BuildIdempotentExtensionFromExisting returns a new extension block with the
// existing entries plus a ProducerID+SeqNum pair appended. If existing is
// nil, returns a block with only the idempotent pair.
// Caller must ReleaseExtensions on the result.
func BuildIdempotentExtensionFromExisting(existing []byte, producerID, seqNum uint64) []byte {
	var existingEntries []byte
	if len(existing) >= ExtHeaderLen {
		totalLen := ExtTotalLen(existing, 0)
		if totalLen > 0 && len(existing) >= ExtHeaderLen+totalLen {
			existingEntries = existing[ExtHeaderLen : ExtHeaderLen+totalLen]
		}
	}

	totalEntries := len(existingEntries) + IdempotentEntrySize
	totalSize := ExtHeaderLen + totalEntries

	b, err := globalSlab.Acquire(totalSize)
	if err != nil {
		b = make([]byte, totalSize)
	} else {
		b = b[:totalSize]
	}

	SetExtTotalLen(b, 0, totalEntries)
	off := ExtHeaderLen
	if len(existingEntries) > 0 {
		copy(b[off:], existingEntries)
		off += len(existingEntries)
	}
	off = SetProducerID(b, off, producerID)
	off = SetSeqNum(b, off, seqNum)
	return b
}
