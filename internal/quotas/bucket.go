package quotas

import (
	"sync"
	"sync/atomic"
	"time"
)

type Bucket struct {
	tokens   atomic.Int64
	capacity int64
	rate     atomic.Int64
	stopCh   chan struct{}
}

func NewBucket(rate int, capacity int) *Bucket {
	b := &Bucket{
		capacity: int64(capacity),
		stopCh:   make(chan struct{}),
	}
	b.rate.Store(int64(rate))
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
			currentRate := b.rate.Load()
			if currentRate <= 0 {
				continue
			}
			added := int64(float64(currentRate) * 0.1)
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

func (b *Bucket) SetRate(rate int) {
	if b != nil {
		b.rate.Store(int64(rate))
	}
}

func (b *Bucket) Stop() {
	if b != nil && b.stopCh != nil {
		close(b.stopCh)
	}
}

type Manager struct {
	mu            sync.Mutex
	bucketsPtr    atomic.Pointer[map[string]*Bucket]
	defaultBucket *Bucket
}

func NewManager(defaultRate, defaultBurst int) *Manager {
	var db *Bucket
	if defaultRate > 0 {
		db = NewBucket(defaultRate, defaultBurst)
	}
	m := &Manager{
		defaultBucket: db,
	}
	initMap := make(map[string]*Bucket)
	m.bucketsPtr.Store(&initMap)
	return m
}

// TryAcquire checks rate limiting for clientID on hot path without acquiring any mutexes (lock-free RCU).
func (m *Manager) TryAcquire(clientID string) bool {
	if m == nil {
		return true
	}

	bucketsMap := m.bucketsPtr.Load()
	var b *Bucket
	if bucketsMap != nil {
		b = (*bucketsMap)[clientID]
	}

	if b == nil {
		b = m.defaultBucket
	}

	return b.TryAcquire()
}

// SetRate updates or creates a client bucket rate dynamically.
func (m *Manager) SetRate(clientID string, rate, burst int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	curMap := m.bucketsPtr.Load()
	if curMap != nil {
		if old, ok := (*curMap)[clientID]; ok {
			old.SetRate(rate)
			return
		}
	}

	newCap := 1
	if curMap != nil {
		newCap += len(*curMap)
	}
	newMap := make(map[string]*Bucket, newCap)
	if curMap != nil {
		for k, v := range *curMap {
			newMap[k] = v
		}
	}
	if burst <= 0 {
		if rate > 0 {
			burst = rate
		} else {
			burst = 1000
		}
	}
	newMap[clientID] = NewBucket(rate, burst)
	m.bucketsPtr.Store(&newMap)
}
