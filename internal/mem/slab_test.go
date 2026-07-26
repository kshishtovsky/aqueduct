package mem

import (
	"testing"
)

func TestSlabAcquireRelease(t *testing.T) {
	sa := New()

	buf, err := sa.Acquire(100)
	if err != nil {
		t.Fatalf("Acquire(100): %v", err)
	}
	if len(buf) != 100 {
		t.Fatalf("expected len=100, got %d", len(buf))
	}
	if cap(buf) != 128 {
		t.Fatalf("expected cap=128, got %d", cap(buf))
	}

	buf[0] = 0xAA
	buf[99] = 0xBB

	sa.Release(buf)
}

func TestSlabAcquireMultiple(t *testing.T) {
	sa := New()

	const n = 1000
	bufs := make([][]byte, n)
	for i := range n {
		b, err := sa.Acquire(64)
		if err != nil {
			t.Fatalf("Acquire(%d): %v", i, err)
		}
		if cap(b) < 64 {
			t.Fatalf("buffer %d: insufficient cap %d", i, cap(b))
		}
		b[0] = byte(i)
		bufs[i] = b
	}

	for i, b := range bufs {
		if b[0] != byte(i) {
			t.Fatalf("buffer %d: data corrupted: got %d", i, b[0])
		}
		sa.Release(b)
	}

	for i := range n {
		b, err := sa.Acquire(64)
		if err != nil {
			t.Fatalf("re-acquire %d: %v", i, err)
		}
		sa.Release(b)
	}
}

func TestSlabNoLeakAfterRelease(t *testing.T) {
	sa := New()

	const n = 10000
	bufs := make([][]byte, n)
	for i := range n {
		b, err := sa.Acquire(200)
		if err != nil {
			t.Fatalf("Acquire(%d): %v", i, err)
		}
		b[0] = byte(i)
		bufs[i] = b
	}

	for _, b := range bufs {
		sa.Release(b)
	}

	for i := range n {
		b, err := sa.Acquire(200)
		if err != nil {
			t.Fatalf("recycle %d: %v", i, err)
		}
		sa.Release(b)
	}
}

func TestSlabSizeClasses(t *testing.T) {
	sa := New()
	sizes := []int{128, 256, 512, 2048, 8192, 32768}

	for _, s := range sizes {
		b, err := sa.Acquire(s)
		if err != nil {
			t.Fatalf("Acquire(%d): %v", s, err)
		}
		if cap(b) != s {
			t.Fatalf("size class %d: expected cap=%d, got %d", s, s, cap(b))
		}
		for i := range s {
			b[i] = byte(i & 0xFF)
		}
		sa.Release(b)
	}
}

func TestSlabAcquireLarge(t *testing.T) {
	sa := New()

	b, err := sa.Acquire(65536)
	if err != nil {
		t.Fatalf("Acquire(65536): %v", err)
	}
	if len(b) != 65536 {
		t.Fatalf("expected len=65536, got %d", len(b))
	}
}

func TestSlabConcurrent(t *testing.T) {
	sa := New()
	done := make(chan struct{})
	const workers = 10
	const iterations = 1000

	for range workers {
		go func() {
			for range iterations {
				b, err := sa.Acquire(50)
				if err != nil {
					panic(err)
				}
				_ = b[0]
				sa.Release(b)
			}
			done <- struct{}{}
		}()
	}

	for range workers {
		<-done
	}
}

func BenchmarkSlabAcquireRelease(b *testing.B) {
	sa := New()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf, err := sa.Acquire(100)
		if err != nil {
			b.Fatal(err)
		}
		sa.Release(buf)
	}
}

func BenchmarkSlabAcquireReleaseContended(b *testing.B) {
	sa := New()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, err := sa.Acquire(100)
			if err != nil {
				b.Fatal(err)
			}
			sa.Release(buf)
		}
	})
}
