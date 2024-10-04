package createcar

import (
	"sync"

	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
)

type StatsSizeByKind struct {
	mu      sync.Mutex
	mapping map[iplddecoders.Kind]uint64
}

func NewStatsSizeByKind() *StatsSizeByKind {
	return &StatsSizeByKind{
		mapping: make(map[iplddecoders.Kind]uint64),
	}
}

func (s *StatsSizeByKind) Add(kind iplddecoders.Kind, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mapping[kind] += size
}

func (s *StatsSizeByKind) Get(kind iplddecoders.Kind) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mapping[kind]
}

func (s *StatsSizeByKind) Total() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total uint64
	for _, size := range s.mapping {
		total += size
	}
	return total
}

func (s *StatsSizeByKind) GetMap() map[iplddecoders.Kind]uint64 {
	cloned := make(map[iplddecoders.Kind]uint64)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.mapping {
		cloned[k] = v
	}
	return cloned
}

func (s *StatsSizeByKind) Infer(data []byte) {
	kind := getKind(data)
	s.Add(kind, uint64(len(data)))
}

func getKind(data []byte) iplddecoders.Kind {
	if len(data) < 2 {
		return -1
	}
	return iplddecoders.Kind(data[1])
}

type CountByDB struct {
	mu        sync.Mutex
	dbToCount map[string]uint64
}

func NewCountByDB() *CountByDB {
	return &CountByDB{
		dbToCount: make(map[string]uint64),
	}
}

func (c *CountByDB) Add(db string, num uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dbToCount[db] += num
}

func (c *CountByDB) Get(db string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dbToCount[db]
}

func (c *CountByDB) GetMap() map[string]uint64 {
	cloned := make(map[string]uint64)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.dbToCount {
		cloned[k] = v
	}
	return cloned
}

func (c *CountByDB) Total() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for _, num := range c.dbToCount {
		total += num
	}
	return total
}

func (c *CountByDB) AddOne(db string) {
	c.Add(db, 1)
}
