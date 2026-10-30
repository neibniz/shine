package redis

import (
	"sync"
	"time"
)

// The simulated database is intentionally ephemeral and bounded. It is not
// used for carrier data or for authentication.
const fakeMaxEntries = 1024
const fakeMaxBytes = 4 << 20

type storeKey struct {
	db  int
	key string
}
type storeValue struct {
	data    []byte
	expires time.Time
}
type fakeStore struct {
	mu   sync.Mutex
	data map[storeKey]storeValue
	size int
}

func newFakeStore() *fakeStore { return &fakeStore{data: make(map[storeKey]storeValue)} }
func (s *fakeStore) deleteLocked(k storeKey) {
	if v, ok := s.data[k]; ok {
		s.size -= len(k.key) + len(v.data)
		delete(s.data, k)
	}
}
func (s *fakeStore) getLocked(k storeKey, now time.Time) (storeValue, bool) {
	v, ok := s.data[k]
	if ok && !v.expires.IsZero() && !now.Before(v.expires) {
		s.deleteLocked(k)
		return storeValue{}, false
	}
	return v, ok
}
func (s *fakeStore) purgeLocked(now time.Time) {
	for k, v := range s.data {
		if !v.expires.IsZero() && !now.Before(v.expires) {
			s.deleteLocked(k)
		}
	}
}
func (s *fakeStore) get(db int, key []byte) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.getLocked(storeKey{db, string(key)}, time.Now())
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v.data...), true
}

// set returns whether the conditional update was applied and whether limits were exceeded.
func (s *fakeStore) set(db int, key, value []byte, mode string, expires time.Time) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	k := storeKey{db, string(key)}
	old, exists := s.getLocked(k, now)
	if mode == "NX" && exists || mode == "XX" && !exists {
		return false, false
	}
	s.purgeLocked(now)
	newSize := s.size + len(key) + len(value)
	if exists {
		newSize -= len(key) + len(old.data)
	}
	if newSize > fakeMaxBytes || (!exists && len(s.data) >= fakeMaxEntries) {
		return false, true
	}
	s.data[k] = storeValue{data: append([]byte(nil), value...), expires: expires}
	s.size = newSize
	return true, false
}
func (s *fakeStore) del(db int, keys [][]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	now := time.Now()
	for _, key := range keys {
		k := storeKey{db, string(key)}
		if _, ok := s.getLocked(k, now); ok {
			s.deleteLocked(k)
			n++
		}
	}
	return n
}
func (s *fakeStore) exists(db int, keys [][]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	now := time.Now()
	for _, key := range keys {
		if _, ok := s.getLocked(storeKey{db, string(key)}, now); ok {
			n++
		}
	}
	return n
}
func (s *fakeStore) count(db int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeLocked(time.Now())
	n := 0
	for k := range s.data {
		if k.db == db {
			n++
		}
	}
	return n
}
func (s *fakeStore) expire(db int, key []byte, d time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := storeKey{db, string(key)}
	now := time.Now()
	v, ok := s.getLocked(k, now)
	if !ok {
		return false
	}
	if d <= 0 {
		s.deleteLocked(k)
		return true
	}
	v.expires = now.Add(d)
	s.data[k] = v
	return true
}
func (s *fakeStore) ttl(db int, key []byte, millis bool) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	v, ok := s.getLocked(storeKey{db, string(key)}, now)
	if !ok {
		return -2
	}
	if v.expires.IsZero() {
		return -1
	}
	d := v.expires.Sub(now)
	if millis {
		return int64((d + time.Millisecond/2) / time.Millisecond)
	}
	return int64((d + time.Second/2) / time.Second)
}

type fakeDBStats struct {
	keys      int
	expires   int
	ttlMillis int64
}

func (s *fakeStore) stats() [16]fakeDBStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.purgeLocked(now)
	var result [16]fakeDBStats
	for k, v := range s.data {
		x := &result[k.db]
		x.keys++
		if !v.expires.IsZero() {
			x.expires++
			x.ttlMillis += v.expires.Sub(now).Milliseconds()
		}
	}
	return result
}
func (s *fakeStore) getMany(db int, keys [][]byte) ([][]byte, []bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	values := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	total := 0
	for i, key := range keys {
		v, ok := s.getLocked(storeKey{db, string(key)}, now)
		if ok {
			if len(v.data) > fakeMaxBytes-total {
				return nil, nil, true
			}
			total += len(v.data)
			values[i] = append([]byte(nil), v.data...)
			found[i] = true
		}
	}
	return values, found, false
}
