package mem

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

var ErrOutOfMemory = errors.New("slab: out of memory")

const noBlock = math.MaxUint64

type sizeClass struct {
	blockSize int
	arena     []byte
	blocks    int
	head      atomic.Uint64
	next      []atomic.Uint64
	mu        sync.Mutex
}

func newSizeClass(blockSize int) *sizeClass {
	sc := &sizeClass{
		blockSize: blockSize,
	}
	sc.head.Store(noBlock)
	return sc
}

func (sc *sizeClass) grow() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.head.Load() != noBlock {
		return nil
	}

	arenaSize := 64 * 1024 * 1024
	blocks := arenaSize / sc.blockSize

	newArena := make([]byte, arenaSize)
	newNext := make([]atomic.Uint64, blocks)

	for i := 0; i < blocks-1; i++ {
		newNext[i].Store(uint64(i + 1))
	}
	newNext[blocks-1].Store(noBlock)

	sc.arena = newArena
	sc.next = newNext
	sc.blocks = blocks
	sc.head.Store(0)
	return nil
}

func (sc *sizeClass) Acquire() ([]byte, error) {
	for {
		head := sc.head.Load()
		if head == noBlock {
			if err := sc.grow(); err != nil {
				return nil, err
			}
			continue
		}
		idx := int(head)
		if idx < 0 || idx >= len(sc.next) {
			return nil, ErrOutOfMemory
		}
		nxt := sc.next[idx].Load()
		if sc.head.CompareAndSwap(head, nxt) {
			offset := idx * sc.blockSize
			end := offset + sc.blockSize
			return sc.arena[offset:end:end], nil
		}
	}
}

func (sc *sizeClass) Release(buf []byte) {
	if len(sc.arena) == 0 {
		return
	}
	arenaStart := uintptr(unsafe.Pointer(unsafe.SliceData(sc.arena)))
	bufStart := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	if bufStart < arenaStart {
		return
	}
	delta := bufStart - arenaStart
	if delta >= uintptr(len(sc.arena)) {
		return
	}
	idx := int(delta / uintptr(sc.blockSize))
	if idx >= sc.blocks {
		return
	}

	for {
		head := sc.head.Load()
		sc.next[idx].Store(head)
		if sc.head.CompareAndSwap(head, uint64(idx)) {
			return
		}
	}
}

type SlabAllocator struct {
	classes []*sizeClass
	sizes   []int
}

func New() *SlabAllocator {
	classes := []int{128, 256, 512, 2048, 8192, 32768}
	sc := make([]*sizeClass, len(classes))
	sz := make([]int, len(classes))
	for i, s := range classes {
		sc[i] = newSizeClass(s)
		sz[i] = s
	}
	return &SlabAllocator{classes: sc, sizes: sz}
}

func (sa *SlabAllocator) Acquire(size int) ([]byte, error) {
	for i, s := range sa.sizes {
		if size <= s {
			buf, err := sa.classes[i].Acquire()
			if err != nil {
				return nil, err
			}
			return buf[:size], nil
		}
	}
	return make([]byte, size), nil
}

func (sa *SlabAllocator) Release(buf []byte) {
	capBuf := cap(buf)
	for i, s := range sa.sizes {
		if capBuf == s {
			sa.classes[i].Release(buf)
			return
		}
	}
}

func (sa *SlabAllocator) ReleaseWithLen(buf []byte, length int) {
	bp := &buf
	_ = bp
	_ = length
	for i, s := range sa.sizes {
		if cap(buf) == s {
			sa.classes[i].Release(buf)
			return
		}
	}
}
