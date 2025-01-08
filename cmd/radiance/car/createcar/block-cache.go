package createcar

import (
	"sync"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

type BlockCache struct {
	rwm   sync.RWMutex
	cache map[uint64]ParsedBlock
}

func NewBlockCache() *BlockCache {
	return &BlockCache{
		cache: make(map[uint64]ParsedBlock),
	}
}

type ParsedBlock map[solana.Signature][]byte

func (c *BlockCache) GetBlock(slot uint64) (ParsedBlock, bool) {
	c.rwm.RLock()
	defer c.rwm.RUnlock()
	block, ok := c.cache[slot]
	return block, ok
}

func (c *BlockCache) SetBlock(slot uint64, block ParsedBlock) {
	c.rwm.Lock()
	defer c.rwm.Unlock()
	c.cache[slot] = block
}

func (c *BlockCache) DeleteBlock(slot uint64) {
	c.rwm.Lock()
	defer c.rwm.Unlock()
	delete(c.cache, slot)
}

// SlotLocks makes sure that a given slot is only downloaded once.
// Thus this checks if a slot is being actively downloaded.
// If it is, it blocks until the download is finished.
// If it is not, it marks the slot as being downloaded.
// This is useful to avoid downloading the same slot multiple times.
type SlotLocks struct {
	mu    sync.Mutex
	locks map[uint64]*Once
}

func NewSlotLocks() *SlotLocks {
	return &SlotLocks{
		locks: make(map[uint64]*Once),
	}
}

// OncePerSlot returns true if the slot was not being downloaded, and it marks the slot as being downloaded.
// If was NOT being downloaded, it runs the given function and then returns true.
// If it was being downloaded, it returns false.
func (s *SlotLocks) OncePerSlot(slot uint64, fn func() error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.locks[slot]; ok {
		return false, nil
	}
	s.locks[slot] = &Once{}
	return s.locks[slot].Do(fn)
}

type Once struct {
	done atomic.Uint32
	m    sync.Mutex
}

// Returns `T, true, error` if the function was executed.
// Returns `T, false, nil` if the function was not executed.
func (o *Once) Do(f func() error) (bool, error) {
	if o.done.Load() == 0 {
		// Outlined slow-path to allow inlining of the fast-path.
		return o.doSlow(f)
	}
	return false, nil
}

func (o *Once) doSlow(f func() error) (bool, error) {
	o.m.Lock()
	defer o.m.Unlock()
	if o.done.Load() == 0 {
		defer o.done.Store(1)
		return true, f()
	}
	return false, nil
}
