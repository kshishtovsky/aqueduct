package quotas

import (
	"sync"
	"sync/atomic"
	"time"
)

type Bucket struct {
	tokens   atomic.Int64
	capacity int64
	rate     int64
	stopCh   chan struct{}
}

func NewBucket(rate int, capacity int) *Bucket {
	b := &Bucket{
		capacity: int64(capacity),
		rate:     int64(rate),
		stopCh:   make(chan struct{}),
	}
	b.tokens.Store(int64(capacity))
	if rate > 0 {
		go b.refillLoop()
	}
	return b
}

func (b *Bucket) refillLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			added := int64(float64(b.rate) * 0.1)
			if added <= 0 {
				added = 1
			}
			for {
				cur := b.tokens.Load()
				want := cur + added
				if want > b.capacity {
					want = b.capacity
				}
				if b.tokens.CompareAndSwap(cur, want) {
					break
				}
			}
		case <-b.stopCh:
			return
		}
	}
}

func (b *Bucket) TryAcquire() bool {
	if b == nil {
		return true
	}
	for {
		cur := b.tokens.Load()
		if cur <= 0 {
			return false
		}
		if b.tokens.CompareAndSwap(cur, cur-1) {
			return true
		}
	}
}

func (b *Bucket) Stop() {
	if b != nil && b.stopCh != nil {
		close(b.stopCh)
	}
}

type Manager struct {
	mu            sync.RWMutex
	buckets       map[string]*Bucket
	defaultBucket *Bucket
}

func NewManager(defaultRate, defaultBurst int) *Manager {
	var db *Bucket
	if defaultRate > 0 {
		db = NewBucket(defaultRate, defaultBurst)
	}
	return &Manager{
		buckets:       make(map[string]*Bucket),
		defaultBucket: db,
	}
}

func (m *Manager) TryAcquire(clientID string) bool {
	if m == nil {
		return true
	}

	m.mu.RLock()
	b, ok := m.buckets[clientID]
	m.mu.RUnlock()

	if !ok {
		b = m.defaultBucket
	}

	return b.TryAcquire()
}

func (m *Manager) SetRate(clientID string, rate, burst int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.buckets[clientID]; ok {
		old.Stop()
	}
	m.buckets[clientID] = NewBucket(rate, burst)
}
