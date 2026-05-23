package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int
	keys     []int          // sorted virtual node hashes
	hashMap  map[int]string // virtual hash → real node addr
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds real node addresses to the hash ring.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Get returns the nearest real node for the given key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	return m.hashMap[m.keys[idx%len(m.keys)]]
}

// GetN returns up to n distinct real nodes starting from the key's
// primary owner and walking clockwise around the ring.
func (m *Map) GetN(key string, n int) []string {
	if len(m.keys) == 0 {
		return nil
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	result := make([]string, 0, n)
	seen := make(map[string]bool)
	for i := 0; i < len(m.keys) && len(result) < n; i++ {
		addr := m.hashMap[m.keys[(idx+i)%len(m.keys)]]
		if !seen[addr] {
			seen[addr] = true
			result = append(result, addr)
		}
	}
	return result
}

// Remove removes a real node from the hash ring.
func (m *Map) Remove(addr string) {
	var newKeys []int
	for _, h := range m.keys {
		if m.hashMap[h] != addr {
			newKeys = append(newKeys, h)
		}
	}
	m.keys = newKeys
	// Clean up hashMap entries for this addr.
	for h, a := range m.hashMap {
		if a == addr {
			delete(m.hashMap, h)
		}
	}
}
