package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestExtTotalLen(t *testing.T) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint16(buf[0:2], 256)
	if got := ExtTotalLen(buf, 0); got != 256 {
		t.Fatalf("ExtTotalLen = %d, want 256", got)
	}
}

func TestSetExtTotalLen(t *testing.T) {
	buf := make([]byte, 4)
	SetExtTotalLen(buf, 0, 512)
	if got := binary.LittleEndian.Uint16(buf[0:2]); got != 512 {
		t.Fatalf("SetExtTotalLen wrote %d, want 512", got)
	}
}

func TestExtBlockSize(t *testing.T) {
	if got := ExtBlockSize(25); got != 27 {
		t.Fatalf("ExtBlockSize(25) = %d, want 27", got)
	}
	if got := ExtBlockSize(0); got != 2 {
		t.Fatalf("ExtBlockSize(0) = %d, want 2", got)
	}
}

func TestParseTLVEntry(t *testing.T) {
	buf := []byte{0x01, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	typ, val, next, err := ParseTLVEntry(buf, 0)
	if err != nil {
		t.Fatalf("ParseTLVEntry error: %v", err)
	}
	if typ != 0x01 {
		t.Fatalf("type = %d, want 0x01", typ)
	}
	if len(val) != 5 || val[0] != 0xAA || val[4] != 0xEE {
		t.Fatalf("val = %v, want [0xAA,0xBB,0xCC,0xDD,0xEE]", val)
	}
	if next != 7 {
		t.Fatalf("next = %d, want 7", next)
	}
}

func TestParseTLVEntryZeroLen(t *testing.T) {
	buf := []byte{0x02, 0x00, 0xFF}
	typ, val, next, err := ParseTLVEntry(buf, 0)
	if err != nil {
		t.Fatalf("ParseTLVEntry error: %v", err)
	}
	if typ != 0x02 {
		t.Fatalf("type = %d, want 0x02", typ)
	}
	if val != nil {
		t.Fatalf("val = %v, want nil", val)
	}
	if next != 2 {
		t.Fatalf("next = %d, want 2", next)
	}
}

func TestParseTLVEntryTruncated(t *testing.T) {
	buf := []byte{0x01}
	_, _, _, err := ParseTLVEntry(buf, 0)
	if err == nil {
		t.Fatal("expected error for truncated entry header")
	}
}

func TestFindExtension(t *testing.T) {
	extBlock := BuildExtensions(
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8},
		1,
	)
	defer ReleaseExtensions(extBlock)

	val, found := FindExtension(extBlock, ExtTraceContext)
	if !found {
		t.Fatal("expected to find trace context extension")
	}
	if len(val) != ExtTraceContextLen {
		t.Fatalf("val len = %d, want %d", len(val), ExtTraceContextLen)
	}

	_, found = FindExtension(extBlock, ExtensionType(0xFF))
	if found {
		t.Fatal("expected not to find unknown type 0xFF")
	}
}

func TestFindExtensionEmptyBlock(t *testing.T) {
	_, found := FindExtension(nil, ExtTraceContext)
	if found {
		t.Fatal("expected not found for nil block")
	}

	_, found = FindExtension([]byte{0, 0}, ExtTraceContext)
	if found {
		t.Fatal("expected not found for empty block (totalLen=0)")
	}
}

func TestExtractTraceContext(t *testing.T) {
	traceID := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	spanID := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	extBlock := BuildExtensions(traceID, spanID, 1)
	defer ReleaseExtensions(extBlock)

	tid, sid, flags, ok := ExtractTraceContext(extBlock)
	if !ok {
		t.Fatal("expected to extract trace context")
	}
	if !bytes.Equal(tid, traceID) {
		t.Fatalf("traceID = %v, want %v", tid, traceID)
	}
	if !bytes.Equal(sid, spanID) {
		t.Fatalf("spanID = %v, want %v", sid, spanID)
	}
	if flags != 1 {
		t.Fatalf("traceFlags = %d, want 1", flags)
	}
}

func TestSetTraceContext(t *testing.T) {
	buf := make([]byte, 27)
	traceID := []byte{0: 0xAA, 15: 0xBB}
	spanID := []byte{0: 0xCC, 7: 0xDD}
	offset := SetTraceContext(buf, 0, traceID, spanID, 1)

	if offset != 27 {
		t.Fatalf("offset = %d, want 27", offset)
	}
	if buf[0] != byte(ExtTraceContext) {
		t.Fatalf("type = %d, want %d", buf[0], ExtTraceContext)
	}
	if buf[1] != ExtTraceContextLen {
		t.Fatalf("len = %d, want %d", buf[1], ExtTraceContextLen)
	}
	if !bytes.Equal(buf[2:18], traceID) {
		t.Fatal("traceID mismatch")
	}
	if !bytes.Equal(buf[18:26], spanID) {
		t.Fatal("spanID mismatch")
	}
	if buf[26] != 1 {
		t.Fatalf("traceFlags = %d, want 1", buf[26])
	}
}

func TestBuildExtensions(t *testing.T) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	for i := range traceID {
		traceID[i] = byte(i)
	}
	for i := range spanID {
		spanID[i] = byte(i + 16)
	}

	extBlock := BuildExtensions(traceID, spanID, 0)
	defer ReleaseExtensions(extBlock)

	if len(extBlock) < ExtHeaderLen {
		t.Fatalf("extBlock too short: %d", len(extBlock))
	}
	totalLen := ExtTotalLen(extBlock, 0)
	expectedTotal := ExtEntryHeader + ExtTraceContextLen
	if totalLen != expectedTotal {
		t.Fatalf("totalLen = %d, want %d", totalLen, expectedTotal)
	}

	tid, sid, flags, ok := ExtractTraceContext(extBlock)
	if !ok {
		t.Fatal("failed to extract trace context from built block")
	}
	if !bytes.Equal(tid, traceID) {
		t.Fatal("traceID mismatch")
	}
	if !bytes.Equal(sid, spanID) {
		t.Fatal("spanID mismatch")
	}
	if flags != 0 {
		t.Fatalf("flags = %d, want 0", flags)
	}
}

func TestFrameWithExtensionsParse(t *testing.T) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	for i := range traceID {
		traceID[i] = byte(i)
	}
	for i := range spanID {
		spanID[i] = byte(i + 100)
	}

	extBlock := BuildExtensions(traceID, spanID, 1)
	defer ReleaseExtensions(extBlock)

	payload := []byte("topic:test.topic")
	bp := SerializeFrameWithExtensions(CmdPublish, 42, extBlock, payload)
	defer ReleaseBuffer(bp)

	frame, err := ParseFrame(*bp)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}

	if !frame.HasExtensions() {
		t.Fatal("expected HasExtensions to be true")
	}
	if frame.StreamID != 42 {
		t.Fatalf("StreamID = %d, want 42", frame.StreamID)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("Payload = %v, want %v", frame.Payload, payload)
	}

	tid, sid, flags, ok := ExtractTraceContext(frame.Extensions)
	if !ok {
		t.Fatal("expected to extract trace context")
	}
	if !bytes.Equal(tid, traceID) {
		t.Fatal("traceID mismatch")
	}
	if !bytes.Equal(sid, spanID) {
		t.Fatal("spanID mismatch")
	}
	if flags != 1 {
		t.Fatalf("flags = %d, want 1", flags)
	}
}

func TestFrameWithoutExtensions(t *testing.T) {
	payload := []byte("topic:test.topic")
	bp := SerializeFrame(CmdPublish, 7, payload)
	defer ReleaseBuffer(bp)

	frame, err := ParseFrame(*bp)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}

	if frame.HasExtensions() {
		t.Fatal("expected HasExtensions to be false")
	}
	if frame.StreamID != 7 {
		t.Fatalf("StreamID = %d, want 7", frame.StreamID)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestBackwardCompatibility(t *testing.T) {
	payload := []byte("topic:old.topic")
	bp := SerializeFrame(CmdPublish, 0, payload)
	defer ReleaseBuffer(bp)

	oldDataLen := binary.LittleEndian.Uint32((*bp)[6:10])
	if int(oldDataLen) != len(payload) {
		t.Fatalf("old DataLen = %d, want %d", oldDataLen, len(payload))
	}

	frame, err := ParseFrame(*bp)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}
	if frame.Extensions != nil {
		t.Fatal("expected nil extensions for frame without HasExtensions bit")
	}
}

func TestFrameDataLen(t *testing.T) {
	payload := []byte("topic:test")
	extBlock := BuildExtensions(
		make([]byte, 16), make([]byte, 8), 0,
	)
	defer ReleaseExtensions(extBlock)

	bp := SerializeFrameWithExtensions(CmdPublish, 0, extBlock, payload)
	defer ReleaseBuffer(bp)

	frame, err := ParseFrame(*bp)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}

	expectedDataLen := uint32(len(frame.Extensions) + len(frame.Payload))
	if frame.DataLen() != expectedDataLen {
		t.Fatalf("DataLen = %d, want %d", frame.DataLen(), expectedDataLen)
	}
}

func TestSerializeFrameWithExtensions(t *testing.T) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	traceID[0] = 0x0A
	spanID[0] = 0x0B

	extBlock := BuildExtensions(traceID, spanID, 1)
	defer ReleaseExtensions(extBlock)

	payload := []byte("topic:test")
	bp := SerializeFrameWithExtensions(CmdPublish, 99, extBlock, payload)
	defer ReleaseBuffer(bp)

	raw := *bp

	if raw[0] != MagicByte {
		t.Fatalf("magic = %d, want %d", raw[0], MagicByte)
	}
	if raw[1] != byte(CmdPublish)|byte(HasExtensionsBit) {
		t.Fatalf("cmd = %d, want %d", raw[1], byte(CmdPublish)|byte(HasExtensionsBit))
	}

	expectedTotalSize := HeaderSize + ExtBlockSize(ExtEntryHeader+ExtTraceContextLen) + len(payload)
	if len(raw) != expectedTotalSize {
		t.Fatalf("frame size = %d, want %d", len(raw), expectedTotalSize)
	}
}

func TestForwardCompatibility(t *testing.T) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	extBlock := BuildExtensions(traceID, spanID, 0)
	defer ReleaseExtensions(extBlock)

	payload := []byte("topic:test")
	bp := SerializeFrameWithExtensions(CmdPublish, 0, extBlock, payload)
	defer ReleaseBuffer(bp)

	raw := *bp

	_, err := ParseFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ParseFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseTLVEntryPointerStability(t *testing.T) {
	buf := make([]byte, 10)
	buf[0] = 0x01
	buf[1] = 0x05
	copy(buf[2:7], []byte{1, 2, 3, 4, 5})

	_, val, _, err := ParseTLVEntry(buf, 0)
	if err != nil {
		t.Fatalf("ParseTLVEntry error: %v", err)
	}

	valPtr := &val[0]
	buf[2] = 0xFF
	if *valPtr != 0xFF {
		t.Fatal("ParseTLVEntry does not return slice pointing into original buffer")
	}
}

func TestExtractTraceContextInvalid(t *testing.T) {
	_, _, _, ok := ExtractTraceContext(nil)
	if ok {
		t.Fatal("expected not ok for nil block")
	}

	_, _, _, ok = ExtractTraceContext([]byte{0, 1, 0x01, 5, 1, 2, 3, 4, 5})
	if ok {
		t.Fatal("expected not ok for wrong TLV type")
	}
}

func BenchmarkParseFrameWithExtensions(b *testing.B) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	extBlock := BuildExtensions(traceID, spanID, 0)
	defer ReleaseExtensions(extBlock)

	payload := make([]byte, 64)
	bp := SerializeFrameWithExtensions(CmdPublish, 0, extBlock, payload)
	defer ReleaseBuffer(bp)
	raw := *bp

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseFrame(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExtractTraceContext(b *testing.B) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	extBlock := BuildExtensions(traceID, spanID, 0)
	defer ReleaseExtensions(extBlock)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, ok := ExtractTraceContext(extBlock)
		if !ok {
			b.Fatal("extract failed")
		}
	}
}

func BenchmarkFindExtension(b *testing.B) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	extBlock := BuildExtensions(traceID, spanID, 0)
	defer ReleaseExtensions(extBlock)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, found := FindExtension(extBlock, ExtTraceContext)
		if !found {
			b.Fatal("not found")
		}
	}
}

func BenchmarkSerializeFrameWithExtensions(b *testing.B) {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	extBlock := BuildExtensions(traceID, spanID, 0)

	payload := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bp := SerializeFrameWithExtensions(CmdPublish, 0, extBlock, payload)
		ReleaseBuffer(bp)
	}

	ReleaseExtensions(extBlock)
}

func TestBuildCompressionExtension(t *testing.T) {
	const uncompressedSize = 4096
	extBlock := BuildCompressionExtension(AlgoZSTD, uncompressedSize)
	defer ReleaseExtensions(extBlock)

	if len(extBlock) < ExtHeaderLen {
		t.Fatalf("extBlock too short: %d", len(extBlock))
	}

	totalLen := ExtTotalLen(extBlock, 0)
	expectedTotal := ExtEntryHeader + ExtCompressionValueLen
	if totalLen != expectedTotal {
		t.Fatalf("totalLen = %d, want %d", totalLen, expectedTotal)
	}

	typ := ExtensionType(extBlock[ExtHeaderLen])
	if typ != ExtCompression {
		t.Fatalf("type = %d, want %d", typ, ExtCompression)
	}

	valLen := int(extBlock[ExtHeaderLen+1])
	if valLen != ExtCompressionValueLen {
		t.Fatalf("value len = %d, want %d", valLen, ExtCompressionValueLen)
	}

	algo := extBlock[ExtHeaderLen+2]
	if algo != AlgoZSTD {
		t.Fatalf("algo = %d, want %d", algo, AlgoZSTD)
	}

	gotSize := binary.LittleEndian.Uint32(extBlock[ExtHeaderLen+3:])
	if gotSize != uncompressedSize {
		t.Fatalf("uncompressedSize = %d, want %d", gotSize, uncompressedSize)
	}

	// Verify FindExtension finds it
	val, found := FindExtension(extBlock, ExtCompression)
	if !found {
		t.Fatal("expected to find compression extension")
	}
	if len(val) != ExtCompressionValueLen {
		t.Fatalf("compression value len = %d, want %d", len(val), ExtCompressionValueLen)
	}
}

func TestStripCompressionExtension(t *testing.T) {
	// Build a block with both TraceContext and Compression
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	ctxBlock := BuildExtensions(traceID, spanID, 1)
	defer ReleaseExtensions(ctxBlock)

	merged := BuildMergedExtensionsWithCompression(ctxBlock, 4096)
	defer ReleaseExtensions(merged)

	// Verify both extensions are present
	_, found := FindExtension(merged, ExtTraceContext)
	if !found {
		t.Fatal("expected trace context after merge")
	}
	_, found = FindExtension(merged, ExtCompression)
	if !found {
		t.Fatal("expected compression after merge")
	}

	// Strip compression
	stripped := StripExtension(merged, ExtCompression)
	if stripped == nil {
		t.Fatal("expected non-nil stripped block (trace context remains)")
	}
	defer ReleaseExtensions(stripped)

	// Verify compression is gone
	_, found = FindExtension(stripped, ExtCompression)
	if found {
		t.Fatal("compression should be stripped")
	}
	// Verify trace context remains
	tid, sid, flags, ok := ExtractTraceContext(stripped)
	if !ok {
		t.Fatal("expected trace context to remain after strip")
	}
	if len(tid) != 16 || len(sid) != 8 || flags != 1 {
		t.Fatal("trace context data mismatch after strip")
	}
}

func TestStripCompressionExtensionOnly(t *testing.T) {
	// Build block with ONLY compression
	extBlock := BuildCompressionExtension(AlgoZSTD, 8192)
	defer ReleaseExtensions(extBlock)

	// Strip compression
	stripped := StripExtension(extBlock, ExtCompression)
	if stripped != nil {
		t.Fatal("expected nil stripped block (no other extensions)")
	}
}

func TestBuildMergedExtensionsWithCompressionEmpty(t *testing.T) {
	// Merging with nil existing should produce compression-only block
	merged := BuildMergedExtensionsWithCompression(nil, 2048)
	if merged == nil {
		t.Fatal("expected non-nil merged block")
	}
	defer ReleaseExtensions(merged)

	_, found := FindExtension(merged, ExtCompression)
	if !found {
		t.Fatal("expected compression after merge with nil")
	}

	val, found := FindExtension(merged, ExtCompression)
	if !found || len(val) < 5 {
		t.Fatal("invalid compression extension after merge")
	}
	algo := val[0]
	uncompSize := binary.LittleEndian.Uint32(val[1:5])
	if algo != AlgoZSTD {
		t.Fatalf("algo = %d, want %d", algo, AlgoZSTD)
	}
	if uncompSize != 2048 {
		t.Fatalf("uncompressedSize = %d, want %d", uncompSize, 2048)
	}
}

func TestDecompressFrameSizeLimit(t *testing.T) {
	// Verify that large uncompressed sizes are properly handled
	extBlock := BuildCompressionExtension(AlgoZSTD, 1<<20) // 1MB
	defer ReleaseExtensions(extBlock)

	val, found := FindExtension(extBlock, ExtCompression)
	if !found {
		t.Fatal("expected compression extension")
	}
	uncompSize := binary.LittleEndian.Uint32(val[1:5])
	if uncompSize != 1<<20 {
		t.Fatalf("uncompressedSize = %d, want %d", uncompSize, 1<<20)
	}
}

func TestOpcodeOfStripsHasExtensions(t *testing.T) {
	cmd := Command(CmdPublish) | HasExtensionsBit | MeshForwardedBit
	opcode := OpcodeOf(cmd)
	if opcode != CmdPublish {
		t.Fatalf("OpcodeOf = %d, want %d", opcode, CmdPublish)
	}
}

func BenchmarkParseFrameBackwardCompat(b *testing.B) {
	payload := make([]byte, 64)
	bp := SerializeFrame(CmdPublish, 0, payload)
	defer ReleaseBuffer(bp)
	raw := *bp

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseFrame(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}
