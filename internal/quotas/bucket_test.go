package quotas

import (
	"sync"
	"testing"
	"time"
)

func TestBucketTryAcquire(t *testing.T) {
	b := NewBucket(1000000, 1000)
	defer b.Stop()

	for range 1000 {
		if !b.TryAcquire() {
			t.Fatal("expected acquire to succeed within capacity")
		}
	}

	if b.TryAcquire() {
		t.Fatal("expected acquire to fail after capacity exhausted")
	}
}

func TestBucketRefill(t *testing.T) {
	b := NewBucket(10000, 1000)
	defer b.Stop()

	for range 1000 {
		b.TryAcquire()
	}

	if b.TryAcquire() {
		t.Fatal("expected tokens to be exhausted immediately after using burst")
	}

	time.Sleep(150 * time.Millisecond)

	if !b.TryAcquire() {
		t.Fatal("expected refill after time passes")
	}
}

func TestManagerDefaultRate(t *testing.T) {
	m := NewManager(10000, 10)

	for range 10 {
		if !m.TryAcquire("unknown-client") {
			t.Fatal("expected default rate to allow within burst")
		}
	}

	if m.TryAcquire("unknown-client") {
		t.Fatal("expected rate limit after burst exhausted")
	}
}

func TestManagerPerClientOverride(t *testing.T) {
	m := NewManager(10, 10)

	m.SetRate("fast-client", 1000000, 1000000)

	for range 500 {
		if !m.TryAcquire("fast-client") {
			t.Fatalf("expected fast-client to have higher rate, denied at iteration")
		}
	}
}

func TestManagerNilSafety(t *testing.T) {
	var m *Manager
	if !m.TryAcquire("any") {
		t.Fatal("nil manager should allow all")
	}
}

func TestBucketConcurrent(t *testing.T) {
	b := NewBucket(100000000, 100000)
	defer b.Stop()

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5000 {
				b.TryAcquire()
			}
		}()
	}
	wg.Wait()
}

func BenchmarkTokenBucketCheck(b *testing.B) {
	bkt := NewBucket(100000000, 100000000)
	defer bkt.Stop()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bkt.TryAcquire()
	}
}

func BenchmarkTokenBucketCheckContended(b *testing.B) {
	bkt := NewBucket(100000000, 100000000)
	defer bkt.Stop()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bkt.TryAcquire()
		}
	})
}
