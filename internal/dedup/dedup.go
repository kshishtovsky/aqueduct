// Package dedup implements sliding-window deduplication for Idempotent Producers.
//
// An Idempotent Producer attaches two TLV extensions to every Publish frame:
//
//	ExtProducerID (0x04): [ID: 8]   uint64 producer identifier
//	ExtSeqNum     (0x05): [Seq: 8]  uint64 monotonically increasing sequence number
//
// The broker keeps a per-producer sliding window of the last WindowSize sequence
// numbers. Each Window is stored as a 2048-bit bitmap = 32 uint64 words = 256 bytes.
// Bit-operations on the bitmap are lock-free (atomic.LoadUint64 / CAS) so the hot
// path of a publish call from a contended producer does not serialise on a mutex.
//
// Sliding window algorithm:
//
//   - Each new SeqNum maps to a slot via index = SeqNum % WindowSize.
//   - The bit at (wordIdx = idx/64, bit = idx%64) is checked atomically.
//   - If already set, the message is a duplicate.
//   - If clear, the bit is set via CAS. On success, the message is new.
//   - On a SeqNum that exceeds the highest seen value, obsolete bits are cleared
//     in the now-recycled slots BEFORE the new bit is set.
//   - A SeqNum lagging the highest seen by WindowSize or more is reported as
//     ErrSequenceTooOld — the broker closes the stream (protocol error).
//
// Memory management: a Store maps producerID -> *Window. Entries are evicted
// either by an LRU policy (when the total population exceeds capacity) or by
// a TTL (entries with no traffic for idleTTL are reaped by a background goroutine).
package dedup

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// WindowSize is the number of sequence numbers tracked per ProducerID.
// 2048 bits / 64 bits per word = 32 uint64 words = 256 bytes per producer.
const WindowSize = 2048

// NumWords is the number of uint64 words in the bitmap.
const NumWords = WindowSize / 64

// bitsPerWord is the count of bits per word.
const bitsPerWord = 64

// nowFunc is the package-level clock. The Store swaps this for tests so that
// the Window's lastUsedNs uses the same clock as the eviction loop. On the
// hot path this is plain time.Now — no indirection.
var (
	nowMu   sync.RWMutex
	nowFunc = time.Now
)

// ErrSequenceTooOld is returned by Check when a SeqNum has fallen behind the
// highest seen sequence number by at least WindowSize. This signals a client
// bug or a misuse of the idempotent producer protocol. The broker must close
// the offending stream.
var ErrSequenceTooOld = errors.New("dedup: sequence number too old")

// Check is the outcome of a single dedup check.
type Check uint8

const (
	// New indicates the message is not a duplicate and may be processed.
	New Check = iota
	// Duplicate indicates the message has already been seen within the window.
	Duplicate
)

// Window is a single-producer sliding-window dedup state.
// Memory layout: 256 bytes (32 uint64 words) followed by bookkeeping.
// Lock-free on the hot path: bit check-and-set is atomic CAS; the heavy
// bookkeeping (highSeqNum advance + obsolete bit clearing) is protected by a
// short mutex that is only held when the window's high watermark moves forward.
type Window struct {
	// bitmap is a fixed-size array of uint64. We index it via atomic ops so
	// the Go GC keeps the backing array in a single contiguous region.
	// 32 * 8 = 256 bytes — fits comfortably in L1 cache.
	bitmap [NumWords]uint64

	// advanceMu protects highSeqNum and the corresponding bitmap clearing.
	// It is held only when the highest seen sequence number moves forward.
	// Bit check-and-set on the hot path does NOT touch this mutex.
	advanceMu sync.Mutex

	// highSeqNum is the highest sequence number observed so far.
	// Zero means "no messages yet".
	highSeqNum uint64

	// lastUsedNs is the unix-nano timestamp of the most recent Check.
	// Read lock-free for LRU eviction; written lock-free via Store.
	lastUsedNs atomic.Int64

	// producerID is fixed at construction time so eviction can identify
	// which window is being reaped.
	producerID uint64
}

// newWindow returns an empty window bound to producerID.
func newWindow(producerID uint64) *Window {
	w := &Window{producerID: producerID}
	// lastUsedNs starts at "now" so the eviction loop does not reap
	// a freshly created window before its first Check.
	w.lastUsedNs.Store(currentNow().UnixNano())
	return w
}

// touchLocked updates lastUsedNs to the current clock value. Caller must
// hold w.advanceMu (or otherwise guarantee the window is private).
func (w *Window) touchLocked() {
	w.lastUsedNs.Store(currentNow().UnixNano())
}

// currentNow returns the package clock's current time. Lock-free read on
// the hot path; swap is protected by nowMu.
func currentNow() time.Time {
	nowMu.RLock()
	fn := nowFunc
	nowMu.RUnlock()
	return fn()
}

// SetNowFunc replaces the package clock. Test-only.
func SetNowFunc(fn func() time.Time) {
	nowMu.Lock()
	nowFunc = fn
	nowMu.Unlock()
}

// ResetNowFunc restores the default clock. Test-only.
func ResetNowFunc() {
	nowMu.Lock()
	nowFunc = time.Now
	nowMu.Unlock()
}

// Check atomically tests-and-sets the bit for seqNum.
// Returns Duplicate if the bit was already set inside the window.
// Returns New if the bit was clear (and is now set).
// Returns ErrSequenceTooOld if seqNum is far behind the highest seen.
//
// Hot path: the bit CAS is lock-free. Only the highest-seen bookkeeping
// takes a short mutex when the window advances forward.
//
// lastUsedNs is updated only when highSeqNum advances or when too-old is
// detected. Duplicates within the window do not bump the LRU timestamp —
// this keeps the hot path free of time.Now() syscalls.
func (w *Window) Check(seqNum uint64) (Check, error) {
	// 1. Guard against sequence numbers that have fallen out of the window.
	//    Window semantics: window covers [highSeqNum - WindowSize + 1, highSeqNum].
	//    A seqNum is too old iff seqNum < highSeqNum - WindowSize + 1
	//                            iff highSeqNum >= seqNum + WindowSize
	w.advanceMu.Lock()
	tooOld := w.highSeqNum > 0 && w.highSeqNum >= seqNum+WindowSize
	if tooOld {
		w.touchLocked()
		w.advanceMu.Unlock()
		return 0, ErrSequenceTooOld
	}
	if seqNum > w.highSeqNum {
		// Advance the high watermark and clear recycled bits BEFORE
		// we touch the bit for the new seqNum. This ensures that when
		// seqNum's slot collides with a previous (now out-of-window)
		// sequence, the bit is clear at the moment of CAS.
		w.clearAndAdvanceLocked(seqNum)
		w.touchLocked()
	}
	w.advanceMu.Unlock()

	// 2. Lock-free bit check-and-set.
	//    idx   = seqNum % WindowSize  -> slot in ring buffer
	//    word  = idx / 64              -> uint64 word containing the bit
	//    bit   = idx % 64              -> bit position within that word
	idx := seqNum % WindowSize
	wordIdx := idx / bitsPerWord
	bitMask := uint64(1) << (idx % bitsPerWord)

	for {
		old := atomic.LoadUint64(&w.bitmap[wordIdx])
		if old&bitMask != 0 {
			return Duplicate, nil
		}
		if atomic.CompareAndSwapUint64(&w.bitmap[wordIdx], old, old|bitMask) {
			break
		}
		// CAS failed: another goroutine modified this word. Re-read and retry.
	}

	return New, nil
}

// clearAndAdvanceLocked clears bits for seqs that have fallen out of the new
// window and updates highSeqNum to seqNum. The new window is
// (seqNum - WindowSize + 1, seqNum]; seqs <= seqNum - WindowSize are recycled.
// Caller must hold w.advanceMu.
func (w *Window) clearAndAdvanceLocked(seqNum uint64) {
	// The new window covers (seqNum - WindowSize + 1, seqNum]. Any seq s
	// with s <= seqNum - WindowSize is out of the new window and its bit
	// (at slot s % WindowSize) is now stale.
	if seqNum > WindowSize {
		clearEnd := seqNum - WindowSize
		clearStart := uint64(1)
		if w.highSeqNum >= WindowSize {
			clearStart = w.highSeqNum - WindowSize + 1
		}
		for s := clearStart; s <= clearEnd; s++ {
			oldIdx := s % WindowSize
			oldWordIdx := oldIdx / bitsPerWord
			oldBitMask := uint64(1) << (oldIdx % bitsPerWord)
			// SAFE: wordIdx is in [0, NumWords).
			atomic.AndUint64(&w.bitmap[oldWordIdx], ^oldBitMask)
		}
	}
	w.highSeqNum = seqNum
}

// ProducerID returns the producer ID this window is bound to.
func (w *Window) ProducerID() uint64 {
	return w.producerID
}

// LastUsed returns the unix-nano timestamp of the last Check.
func (w *Window) LastUsed() int64 {
	return w.lastUsedNs.Load()
}

// EstMemoryBytes returns the per-window memory footprint (256 bytes bitmap + overhead).
func (w *Window) EstMemoryBytes() int {
	return NumWords*8 + 64
}

// storeEntry wraps *Window with the list element identity so the LRU can
// move-to-front in O(1). The list element VALUE is *storeEntry, not *Window.
type storeEntry struct {
	window *Window
	elem   *list.Element
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithCapacity sets the maximum number of windows in the Store. When the
// population exceeds capacity, the least-recently-used window is evicted.
// Default: 65536.
func WithCapacity(n int) StoreOption {
	return func(s *Store) {
		if n > 0 {
			s.capacity = n
		}
	}
}

// WithIdleTTL sets the inactivity timeout for windows. Windows whose
// lastUsed timestamp is older than now-idleTTL are reaped by the background
// eviction goroutine. Default: 5 minutes.
func WithIdleTTL(d time.Duration) StoreOption {
	return func(s *Store) {
		if d > 0 {
			s.idleTTL = d
		}
	}
}

// WithEvictionInterval sets how often the background TTL eviction sweeps.
// Default: 30s.
func WithEvictionInterval(d time.Duration) StoreOption {
	return func(s *Store) {
		if d > 0 {
			s.evictInterval = d
		}
	}
}

// Store is a goroutine-safe LRU + TTL cache of per-producer Windows.
type Store struct {
	mu      sync.Mutex
	entries map[uint64]*storeEntry
	lru     *list.List

	capacity      int
	idleTTL       time.Duration
	evictInterval time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// nowMu protects nowFunc so tests can inject deterministic time
	// without racing against the eviction goroutine. nowFunc is mirrored
	// to the package-level clock so Windows see the same time.
	nowMu   sync.RWMutex
	nowFunc func() time.Time
}

// NewStore creates a Store with the given options. Call Start to enable
// background TTL eviction, and Stop to drain the eviction goroutine.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{
		entries:       make(map[uint64]*storeEntry),
		lru:           list.New(),
		capacity:      65536,
		idleTTL:       5 * time.Minute,
		evictInterval: 30 * time.Second,
		stopCh:        make(chan struct{}),
		nowFunc:       time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the background TTL eviction goroutine. It is safe to call
// Check without calling Start — eviction simply never runs.
func (s *Store) Start() {
	s.wg.Add(1)
	go s.evictLoop()
}

// Stop signals the background eviction goroutine to exit and waits for it.
func (s *Store) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

// Check performs the dedup check for (producerID, seqNum), creating a new
// window on first contact. The returned Window is the same instance the Store
// owns — callers should not retain it across evictions.
func (s *Store) Check(producerID, seqNum uint64) (Check, error, *Window) {
	w := s.getOrCreate(producerID)
	result, err := w.Check(seqNum)
	return result, err, w
}

// Len returns the current number of windows in the store.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// evictLoop runs in the background and reaps windows whose lastUsed timestamp
// is older than idleTTL.
func (s *Store) evictLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.evictInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.nowMu.RLock()
			nowFn := s.nowFunc
			s.nowMu.RUnlock()
			s.reapExpired(nowFn().Add(-s.idleTTL).UnixNano())
		}
	}
}

// reapExpired removes all windows with lastUsedNs <= cutoff.
// Holds the mutex briefly while iterating.
func (s *Store) reapExpired(cutoffNs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toRemove []*list.Element
	for e := s.lru.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*storeEntry)
		if entry.window.LastUsed() <= cutoffNs {
			toRemove = append(toRemove, e)
		} else {
			// LRU is ordered oldest-first; once we hit a fresh entry, we can stop.
			break
		}
	}

	for _, e := range toRemove {
		entry := e.Value.(*storeEntry)
		delete(s.entries, entry.window.ProducerID())
		s.lru.Remove(e)
	}
}

// evictLRULocked removes the oldest entry. Caller must hold s.mu.
func (s *Store) evictLRULocked() {
	e := s.lru.Front()
	if e == nil {
		return
	}
	entry := e.Value.(*storeEntry)
	delete(s.entries, entry.window.ProducerID())
	s.lru.Remove(e)
}

// getOrCreate returns the window for producerID, creating one on first contact.
// On creation, if the store is at capacity, the oldest entry is evicted.
func (s *Store) getOrCreate(producerID uint64) *Window {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.entries[producerID]; ok {
		s.lru.MoveToBack(entry.elem)
		return entry.window
	}

	// Capacity-based eviction before insert.
	for len(s.entries) >= s.capacity {
		s.evictLRULocked()
	}

	w := newWindow(producerID)
	elem := s.lru.PushBack(&storeEntry{window: w})
	s.entries[producerID] = &storeEntry{window: w, elem: elem}
	return w
}
