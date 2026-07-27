package dedup

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// End-to-end Store behaviour
// ---------------------------------------------------------------------------

// TestStore_MultipleProducersIsolated verifies the bitmap-based per-producer
// isolation: two producers with overlapping seqs are tracked independently.
func TestStore_MultipleProducersIsolated(t *testing.T) {
	s := NewStore()
	defer s.Stop()

	// Producer 1 sends seq 1..5
	for i := uint64(1); i <= 5; i++ {
		r, err, _ := s.Check(1, i)
		if err != nil || r != New {
			t.Fatalf("p1 seq %d: r=%v err=%v", i, r, err)
		}
	}

	// Producer 2 sends seq 1..5 — must be all New (different producer).
	for i := uint64(1); i <= 5; i++ {
		r, err, _ := s.Check(2, i)
		if err != nil || r != New {
			t.Fatalf("p2 seq %d: r=%v err=%v", i, r, err)
		}
	}

	// Re-send from p1 — all Duplicate.
	for i := uint64(1); i <= 5; i++ {
		r, err, _ := s.Check(1, i)
		if err != nil || r != Duplicate {
			t.Fatalf("p1 resend %d: r=%v err=%v", i, r, err)
		}
	}

	if s.Len() != 2 {
		t.Fatalf("expected 2 windows, got %d", s.Len())
	}
}

// TestStore_AdversarialOldSequence ensures the protocol-violation path is hit
// when a producer sends a seq that has fallen out of the window.
func TestStore_AdversarialOldSequence(t *testing.T) {
	s := NewStore()
	defer s.Stop()

	// Producer sends seq 5000 first.
	if r, err, _ := s.Check(1, 5000); r != New || err != nil {
		t.Fatalf("seq 5000: r=%v err=%v", r, err)
	}

	// Then sends seq 10 — too old, must error.
	if r, err, _ := s.Check(1, 10); r != New || err != ErrSequenceTooOld {
		t.Fatalf("seq 10: expected ErrSequenceTooOld, got r=%v err=%v", r, err)
	}
}

// TestStore_WindowSlidesOnAdvancingProducer verifies that an advancing
// producer's window is fully populated and the in-window seqs are duplicates.
func TestStore_WindowSlidesOnAdvancingProducer(t *testing.T) {
	s := NewStore()
	defer s.Stop()

	const total uint64 = 3000 // > WindowSize so part of the initial seqs are out-of-window
	for i := uint64(1); i <= total; i++ {
		r, err, _ := s.Check(1, i)
		if err != nil || r != New {
			t.Fatalf("seq %d: r=%v err=%v", i, r, err)
		}
	}

	// In-window seqs (total-100..total) must be Duplicate.
	for i := total - 100; i <= total; i++ {
		r, err, _ := s.Check(1, i)
		if err != nil {
			t.Errorf("seq %d unexpected err: %v", i, err)
			continue
		}
		if r != Duplicate {
			t.Errorf("seq %d expected Duplicate, got %v", i, r)
		}
	}

	// Out-of-window seqs (1..total-WindowSize) must be TooOld.
	outOfWindowEnd := total - WindowSize
	if outOfWindowEnd > 0 {
		for i := uint64(1); i <= outOfWindowEnd; i++ {
			_, err, _ := s.Check(1, i)
			if err != ErrSequenceTooOld {
				t.Errorf("seq %d expected ErrSequenceTooOld, got %v", i, err)
			}
		}
	}
}

// TestStore_ConcurrentPublishers exercises the lock-free fast path under
// concurrent load: many producers sending many seqs.
func TestStore_ConcurrentPublishers(t *testing.T) {
	s := NewStore()
	defer s.Stop()

	const producers = 32
	const seqsPerProducer = 100

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		producerID := uint64(p + 1)
		go func() {
			defer wg.Done()
			for i := uint64(1); i <= seqsPerProducer; i++ {
				r, err, _ := s.Check(producerID, i)
				if err != nil {
					t.Errorf("producer %d seq %d: err=%v", producerID, i, err)
					return
				}
				if r != New {
					t.Errorf("producer %d seq %d: expected New, got %v", producerID, i, r)
					return
				}
			}
		}()
	}
	wg.Wait()

	if s.Len() != producers {
		t.Fatalf("expected %d windows, got %d", producers, s.Len())
	}

	// Re-check: all should be Duplicate.
	for p := 0; p < producers; p++ {
		producerID := uint64(p + 1)
		for i := uint64(1); i <= seqsPerProducer; i++ {
			r, err, _ := s.Check(producerID, i)
			if err != nil {
				t.Errorf("resend p=%d seq=%d: err=%v", producerID, i, err)
				continue
			}
			if r != Duplicate {
				t.Errorf("resend p=%d seq=%d: expected Duplicate, got %v", producerID, i, r)
			}
		}
	}
}

// TestStore_StormSimulation mimics a producer retry storm: 1000 attempts of
// the same seq. The dedup window must accept the first and reject all others.
func TestStore_StormSimulation(t *testing.T) {
	s := NewStore()
	defer s.Stop()

	const attempts = 1000
	var seenNew atomic.Int32
	var seenDup atomic.Int32
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			r, err, _ := s.Check(1, 42)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if r == New {
				seenNew.Add(1)
			} else if r == Duplicate {
				seenDup.Add(1)
			}
		}()
	}
	wg.Wait()

	if seenNew.Load() != 1 {
		t.Fatalf("expected exactly 1 New, got %d", seenNew.Load())
	}
	if seenDup.Load() != attempts-1 {
		t.Fatalf("expected %d Duplicates, got %d", attempts-1, seenDup.Load())
	}
}

// TestStore_ReapingPreservesActiveProducer verifies that the background TTL
// loop reaps idle windows without disturbing active ones.
func TestStore_ReapingPreservesActiveProducer(t *testing.T) {
	var nowNs atomic.Int64
	nowNs.Store(time.Now().UnixNano())
	getNow := func() time.Time {
		return time.Unix(0, nowNs.Load())
	}
	SetNowFunc(getNow)
	defer ResetNowFunc()

	store := NewStore(
		WithCapacity(100),
		WithIdleTTL(50*time.Millisecond),
		WithEvictionInterval(10*time.Millisecond),
	)
	store.nowMu.Lock()
	store.nowFunc = getNow
	store.nowMu.Unlock()
	store.Start()
	defer store.Stop()

	// Create p1 (idle) and p2 (active).
	if r, err, _ := store.Check(1, 1); r != New || err != nil {
		t.Fatalf("p1: r=%v err=%v", r, err)
	}
	if r, err, _ := store.Check(2, 1); r != New || err != nil {
		t.Fatalf("p2: r=%v err=%v", r, err)
	}

	// Advance virtual time past p1's TTL but keep p2 active.
	nowNs.Store(time.Now().Add(100 * time.Millisecond).UnixNano())

	// Re-touch p2 with a new seq, advancing its lastUsed.
	if r, err, _ := store.Check(2, 2); r != New || err != nil {
		t.Fatalf("p2 seq 2: r=%v err=%v", r, err)
	}

	// Wait for eviction cycles.
	deadline := time.Now().Add(2 * time.Second)
	for store.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("expected 1 window left (p2), got %d", store.Len())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// p1 should be gone. Re-adding it creates a fresh window.
	if r, err, _ := store.Check(1, 1); r != New || err != nil {
		t.Fatalf("p1 re-add: r=%v err=%v", r, err)
	}

	// p2 should still be tracked: seq 2 must be Duplicate.
	r, err, _ := store.Check(2, 2)
	if err != nil {
		t.Fatalf("p2 seq 2 resend: err=%v", err)
	}
	if r != Duplicate {
		t.Fatalf("p2 seq 2: expected Duplicate, got %v", r)
	}
}
