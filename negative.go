package gcache

import (
	"sync"
	"time"
)

// negativeCache remembers keys whose loader reported "not found", so a hot missing
// key does not hit the database on every request.
//
// It is deliberately local: the marker exists to protect the source of truth, and
// a per-node marker already does that. It is also short-lived — a negative entry
// that outlives the record it describes turns a creation into a phantom miss.
type negativeCache struct {
	mu      sync.Mutex
	entries map[string]time.Time // key -> expiry
	now     func() time.Time
}

func newNegativeCache(now func() time.Time) *negativeCache {
	if now == nil {
		now = time.Now
	}
	return &negativeCache{entries: map[string]time.Time{}, now: now}
}

// has reports whether the key is negative-cached right now.
func (n *negativeCache) has(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	expiresAt, ok := n.entries[key]
	if !ok {
		return false
	}
	if !n.now().Before(expiresAt) {
		delete(n.entries, key)
		return false
	}
	return true
}

// add records the absence for ttl.
func (n *negativeCache) add(key string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entries[key] = n.now().Add(ttl)
}

// remove clears the marker; every write path calls it, so a value stored after a
// miss is never shadowed by the stale absence.
func (n *negativeCache) remove(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.entries, key)
}

// sweep drops expired markers.
func (n *negativeCache) sweep() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := n.now()
	removed := 0
	for key, expiresAt := range n.entries {
		if !now.Before(expiresAt) {
			delete(n.entries, key)
			removed++
		}
	}
	return removed
}

// size reports the number of live markers.
func (n *negativeCache) size() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.entries)
}
