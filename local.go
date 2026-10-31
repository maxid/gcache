package gcache

import (
	"container/list"
	"sync"
	"time"
)

// entryState is what a local lookup found.
type entryState int

const (
	// stateAbsent means the key is not cached at all.
	stateAbsent entryState = iota
	// stateFresh means the entry is within its TTL.
	stateFresh
	// stateStale means the entry outlived its TTL but is still inside the
	// stale-if-error window: it may only be served when the shared store fails.
	stateStale
)

// localCache is a bounded LRU with per-entry TTL. It is the L1 tier: every read
// either hits here or goes to the shared store.
//
// Values are copied in and out so a caller cannot mutate a cached buffer while
// another goroutine reads it.
type localCache struct {
	maxEntries  int
	maxBytes    int64
	staleWindow time.Duration

	mu    sync.Mutex
	ll    *list.List // front = most recently used
	items map[string]*list.Element
	bytes int64
	now   func() time.Time

	// counters
	hits, misses, evictions, expirations, stale int64
}

type localEntry struct {
	key       string
	value     []byte
	expiresAt time.Time
	version   uint64
}

func newLocalCache(maxEntries int, maxBytes int64, staleWindow time.Duration, now func() time.Time) *localCache {
	if now == nil {
		now = time.Now
	}
	return &localCache{
		maxEntries:  maxEntries,
		maxBytes:    maxBytes,
		staleWindow: staleWindow,
		ll:          list.New(),
		items:       map[string]*list.Element{},
		now:         now,
	}
}

// get returns a copy of the value and whether it is fresh, stale, or absent.
//
// An entry past its TTL is kept for the stale-if-error window instead of being
// dropped immediately: the cache may serve it if the shared store is down, and a
// sweep reclaims it once that window closes.
func (c *localCache) get(key string) (Entry, entryState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		c.misses++
		return Entry{}, stateAbsent
	}

	entry := element.Value.(*localEntry)
	now := c.now()
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		if c.staleWindow <= 0 || !now.Before(entry.expiresAt.Add(c.staleWindow)) {
			c.removeLocked(element)
			c.expirations++
			c.misses++
			return Entry{}, stateAbsent
		}
		// Keep it: stale reads are gated by the caller (remote failure only).
		c.stale++
		c.ll.MoveToFront(element)
		return Entry{Value: cloneBytes(entry.value), ExpiresAt: entry.expiresAt, Version: entry.version, Stale: true}, stateStale
	}

	c.ll.MoveToFront(element)
	c.hits++
	return Entry{Value: cloneBytes(entry.value), ExpiresAt: entry.expiresAt, Version: entry.version}, stateFresh
}

// peek reports presence without disturbing the LRU order or the counters; the
// eviction path uses it to decide whether a delete is needed.
func (c *localCache) peek(key string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		return 0, false
	}
	entry := element.Value.(*localEntry)
	if !entry.expiresAt.IsZero() && !c.now().Before(entry.expiresAt) {
		return 0, false
	}
	return entry.version, true
}

// set stores a copy of the value, evicting until both limits hold.
func (c *localCache) set(key string, value []byte, expiresAt time.Time, version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(key) + len(value))
	if existing, ok := c.items[key]; ok {
		entry := existing.Value.(*localEntry)
		c.bytes += size - int64(len(entry.key)+len(entry.value))
		entry.value = cloneBytes(value)
		entry.expiresAt = expiresAt
		entry.version = version
		c.ll.MoveToFront(existing)
	} else {
		c.items[key] = c.ll.PushFront(&localEntry{
			key:       key,
			value:     cloneBytes(value),
			expiresAt: expiresAt,
			version:   version,
		})
		c.bytes += size
	}

	c.evictLocked()
}

// delete drops one entry, reporting whether it was present.
func (c *localCache) delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		return false
	}
	c.removeLocked(element)
	return true
}

// deleteIfVersion removes the entry only when the stored version is older than
// (or equal to) the invalidation's. A newer local value wins, which is what keeps
// a slow invalidation message from discarding fresher data.
func (c *localCache) deleteIfVersion(key string, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		return false
	}
	entry := element.Value.(*localEntry)
	if version != 0 && entry.version > version {
		return false
	}
	c.removeLocked(element)
	return true
}

// clear empties the tier; the invalidation subscriber uses it when its queue
// overflows, because a missed message means the tier can no longer be trusted.
func (c *localCache) clear() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := len(c.items)
	c.ll.Init()
	c.items = map[string]*list.Element{}
	c.bytes = 0
	return removed
}

// sweep drops expired entries; callers run it on a timer so expired values do not
// keep occupying the budget.
func (c *localCache) sweep() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	removed := 0
	for element := c.ll.Back(); element != nil; {
		previous := element.Prev()
		entry := element.Value.(*localEntry)
		if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
			// Respect the stale window: an entry that may still be served on a
			// store failure stays until the window closes.
			if c.staleWindow > 0 && now.Before(entry.expiresAt.Add(c.staleWindow)) {
				element = previous
				continue
			}
			c.removeLocked(element)
			c.expirations++
			removed++
		}
		element = previous
	}
	return removed
}

func (c *localCache) removeLocked(element *list.Element) {
	entry := element.Value.(*localEntry)
	c.ll.Remove(element)
	delete(c.items, entry.key)
	c.bytes -= int64(len(entry.key) + len(entry.value))
}

func (c *localCache) evictLocked() {
	for (c.maxEntries > 0 && c.ll.Len() > c.maxEntries) || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		oldest := c.ll.Back()
		if oldest == nil {
			return
		}
		c.removeLocked(oldest)
		c.evictions++
	}
}

// stats snapshots the local tier counters.
func (c *localCache) stats() LocalStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return LocalStats{
		Entries:     len(c.items),
		Bytes:       c.bytes,
		Hits:        c.hits,
		Misses:      c.misses,
		Evictions:   c.evictions,
		Expirations: c.expirations,
		Stale:       c.stale,
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
