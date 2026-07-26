package compress

import (
	"bytes"
	"testing"

	"github.com/kshishtovsky/aqueduct/internal/mem"
)

func TestZstdRoundTrip(t *testing.T) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	src := make([]byte, 4096)
	for i := range src {
		src[i] = byte(i & 0xFF)
	}

	compressed, err := engine.Compress(src)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	if len(compressed) >= len(src) {
		t.Logf("compressed size %d >= original %d (expected for incompressible data)", len(compressed), len(src))
	}

	decompressed, err := engine.Decompress(compressed, len(src))
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if !bytes.Equal(decompressed, src) {
		t.Fatal("round-trip data mismatch")
	}

	engine.ReleaseBuf(compressed)
}

func TestZstdRoundTripCompressible(t *testing.T) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	// Highly compressible repetitive data
	src := bytes.Repeat([]byte("hello world this is a compressible message "), 100)

	compressed, err := engine.Compress(src)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	ratio := float64(len(compressed)) / float64(len(src))
	t.Logf("compression ratio: %.2f (%d -> %d)", ratio, len(src), len(compressed))

	if ratio > 0.8 {
		t.Errorf("expected compression ratio < 0.8 for highly compressible data, got %.2f", ratio)
	}

	decompressed, err := engine.Decompress(compressed, len(src))
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if !bytes.Equal(decompressed, src) {
		t.Fatal("round-trip data mismatch")
	}

	engine.ReleaseBuf(compressed)
}

func TestZstdCorruptedPayload(t *testing.T) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	corrupted := []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x00, 0x00, 0x00}
	_, err := engine.Decompress(corrupted, 100)
	if err == nil {
		t.Fatal("expected error for corrupted compressed data, got nil")
	}
}

func TestZstdSmallPayload(t *testing.T) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	// Payload below 1KB threshold — compression should still work
	src := []byte("small payload")

	compressed, err := engine.Compress(src)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := engine.Decompress(compressed, len(src))
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if !bytes.Equal(decompressed, src) {
		t.Fatal("round-trip data mismatch")
	}

	engine.ReleaseBuf(compressed)
}

func TestCompressedSize(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{0, 0},
		{1, 1 + 16 + 16},
		{100, 100 + 16 + 16},
		{1000, 1000 + 16 + 16},
		{4096, 4096 + 16 + 16},
		{65536, 65536 + 257 + 16},
	}
	for _, tt := range tests {
		got := CompressedSize(tt.input)
		if got < tt.input && tt.input > 0 {
			t.Errorf("CompressedSize(%d) = %d, want >= %d", tt.input, got, tt.input)
		}
		if got != tt.want {
			t.Logf("CompressedSize(%d) = %d (expected %d)", tt.input, got, tt.want)
		}
	}
}

func TestZstdEmptyPayload(t *testing.T) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	compressed, err := engine.Compress(nil)
	if err != nil {
		t.Fatalf("Compress nil error: %v", err)
	}

	decompressed, err := engine.Decompress(compressed, 0)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if len(decompressed) != 0 {
		t.Fatalf("expected empty decompressed, got %d bytes", len(decompressed))
	}

	engine.ReleaseBuf(compressed)
}

func BenchmarkZSTDEncode(b *testing.B) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	src := make([]byte, 4096)
	for i := range src {
		src[i] = byte(i & 0xFF)
	}

	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, err := engine.Compress(src)
		if err != nil {
			b.Fatal(err)
		}
		engine.ReleaseBuf(compressed)
	}
}

func BenchmarkZSTDDecode(b *testing.B) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	src := bytes.Repeat([]byte("hello world this is compressible data for zstd benchmark "), 64) // ~4KB
	compressed, err := engine.Compress(src)
	if err != nil {
		b.Fatalf("Compress error: %v", err)
	}
	uncompressedSize := len(src)

	b.SetBytes(int64(uncompressedSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decompressed, err := engine.Decompress(compressed, uncompressedSize)
		if err != nil {
			b.Fatal(err)
		}
		_ = decompressed
	}
}

func BenchmarkZSTDEncodeCompressible(b *testing.B) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	src := bytes.Repeat([]byte("highly compressible data pattern for zstd benchmark "), 80) // ~4KB

	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, err := engine.Compress(src)
		if err != nil {
			b.Fatal(err)
		}
		engine.ReleaseBuf(compressed)
	}
}

// BenchmarkZSTDEncodeLarge benchmarks 64KB batch compression.
func BenchmarkZSTDEncodeLarge(b *testing.B) {
	slab := mem.New()
	engine := NewZstdEngine(slab)

	src := make([]byte, 64*1024)
	_ = src[0]

	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, err := engine.Compress(src)
		if err != nil {
			b.Fatal(err)
		}
		engine.ReleaseBuf(compressed)
	}
}
