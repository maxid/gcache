package gcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gns"
)

// Entry is one cached value. Value is a read-only byte view; callers get their own
// copy. Version is produced by the shared store (or left zero) and guards against
// writing an older value back over a newer one.
type Entry struct {
	Value     []byte
	ExpiresAt time.Time
	Version   uint64
	// Stale marks a value served past its TTL because the shared store failed.
	// It is only ever true when stale-if-error is configured.
	Stale bool
}

// Loader loads a value on a miss. Returning ErrNotFound records a short negative
// entry; any other error is a failure and is never cached.
type Loader func(ctx context.Context) (value []byte, ttl time.Duration, err error)

// RemoteStore is the shared tier (L2). Implementations live in subpackages
// (redis, olric, memory) so the core stays dependency-free.
//
// Version semantics: a store that can produce monotonic versions should do so;
// the cache only writes a local entry whose version is not older than what it
// already holds. A store without versions may return 0 and the cache degrades to
// TTL + invalidation.
type RemoteStore interface {
	Get(ctx context.Context, key string) (Entry, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	// Close releases connections.
	Close() error
}

// Invalidation is one broadcast message. It never carries the value: only the key
// and a version, so a compromised channel cannot leak payloads.
type Invalidation struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   uint64 `json:"version,omitempty"`
	Origin    string `json:"origin,omitempty"`
	EventID   string `json:"eventId,omitempty"`
}

// Invalidator publishes and subscribes to invalidation messages.
//
// Delivery is best effort: pub/sub may drop messages, which is why every entry
// also has a TTL. An implementation that cannot deliver must report the error so
// the cache can return InvalidationError{Applied: true}.
type Invalidator interface {
	Publish(ctx context.Context, message Invalidation) error
	// Subscribe delivers messages until ctx is cancelled.
	Subscribe(ctx context.Context, handler func(Invalidation)) error
	Close() error
}

// Cache is the two-level cache.
type Cache struct {
	cfg     Config
	local   *localCache
	remote  RemoteStore
	inv     Invalidator
	origin  string
	clock   func() time.Time
	metrics counters

	loads    *group
	negative *negativeCache

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// Option configures a Cache.
type Option func(*Cache)

// WithRemote installs the shared tier. Without it the cache is local-only, which
// is useful in tests and single-process deployments.
func WithRemote(store RemoteStore) Option { return func(c *Cache) { c.remote = store } }

// WithInvalidator installs the broadcast channel.
func WithInvalidator(inv Invalidator) Option { return func(c *Cache) { c.inv = inv } }

// WithOrigin labels this instance in invalidation messages so a subscriber can
// ignore its own writes.
func WithOrigin(origin string) Option { return func(c *Cache) { c.origin = origin } }

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option {
	return func(c *Cache) {
		if now != nil {
			c.clock = now
		}
	}
}

// New builds a cache from a configuration.
func New(cfg Config, opts ...Option) (*Cache, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Cache{
		cfg:    cfg,
		remote: nil,
		origin: "local",
		clock:  time.Now,
		ctx:    ctx,
		cancel: cancel,
	}
	for _, o := range opts {
		o(c)
	}
	c.local = newLocalCache(cfg.LocalMaxEntries, cfg.LocalMaxBytes, cfg.StaleIfError, c.clock)
	c.loads = newGroup()
	c.negative = newNegativeCache(c.clock)

	if c.inv != nil {
		c.wg.Add(1)
		go c.subscribe()
	}
	if cfg.SweepInterval != 0 {
		c.wg.Add(1)
		go c.sweepLoop(cfg.SweepInterval)
	}
	return c, nil
}

// sweepLoop reclaims expired entries so they stop occupying the budget between
// reads. It runs at the configured interval and exits with the cache.
func (c *Cache) sweepLoop(interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.local.sweep()
			c.negative.sweep()
		}
	}
}

// Close stops the subscriber, drops the local tier and closes the ports.
func (c *Cache) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.cancel()
	c.wg.Wait()

	var first error
	if c.inv != nil {
		if err := c.inv.Close(); err != nil {
			first = err
		}
	}
	if c.remote != nil {
		if err := c.remote.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Get returns a value from L1, then L2, backfilling L1 on a hit.
//
// When the shared store fails and stale-if-error is configured, an expired local
// entry is served instead of failing, with Entry.Stale set. That is the only path
// that returns a stale value; it is never silent.
func (c *Cache) Get(ctx context.Context, key string) (Entry, bool, error) {
	if c.closed.Load() {
		return Entry{}, false, ErrClosed
	}
	name := c.key(key)

	entry, state := c.local.get(name)
	switch state {
	case stateFresh:
		c.metrics.localHits.Add(1)
		return entry, true, nil
	case stateStale:
		// Not a hit yet: only a failed store turns this into an answer.
		if c.remote == nil {
			c.metrics.localMisses.Add(1)
			return Entry{}, false, nil
		}
	}

	if state == stateAbsent {
		c.metrics.localMisses.Add(1)
	}
	if c.remote == nil {
		return Entry{}, false, nil
	}

	remoteEntry, ok, err := c.remote.Get(ctx, name)
	if err != nil {
		c.metrics.remoteErrors.Add(1)
		if state == stateStale {
			c.metrics.staleServed.Add(1)
			return entry, true, nil
		}
		return Entry{}, false, err
	}
	if !ok {
		c.metrics.remoteMisses.Add(1)
		if state == stateStale {
			// The store answered authoritatively: the value is gone.
			c.local.delete(name)
		}
		return Entry{}, false, nil
	}

	c.metrics.remoteHits.Add(1)
	if len(remoteEntry.Value) > int(c.cfg.RemoteMaxValueBytes) {
		// Too large to hold locally; still a valid answer for this call.
		return remoteEntry, true, nil
	}
	c.local.set(name, remoteEntry.Value, remoteEntry.ExpiresAt, remoteEntry.Version)
	return remoteEntry, true, nil
}

// Set writes L2 first, then L1, then broadcasts. A broadcast failure returns
// InvalidationError{Applied: true}: the write took effect, other nodes may serve a
// stale copy until TTL expiry. It must not be treated as "not written".
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if int64(len(value)) > c.cfg.RemoteMaxValueBytes {
		return fmt.Errorf("%w: %d bytes exceed %d", ErrTooLarge, len(value), c.cfg.RemoteMaxValueBytes)
	}

	name := c.key(key)
	ttl = c.effectiveTTL(ttl)

	if c.remote != nil {
		if err := c.remote.Set(ctx, name, value, ttl); err != nil {
			return err
		}
	}
	c.local.set(name, value, c.clock().Add(ttl), 0)
	c.negative.remove(name)

	return c.invalidate(ctx, name, 0)
}

// Delete removes from L2, then L1, then broadcasts.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	name := c.key(key)

	if c.remote != nil {
		if err := c.remote.Delete(ctx, name); err != nil {
			return err
		}
	}
	c.local.delete(name)
	c.negative.remove(name)
	return c.invalidate(ctx, name, 0)
}

// GetOrLoad returns a cached value or loads it, collapsing concurrent misses for
// the same key into one loader run.
func (c *Cache) GetOrLoad(ctx context.Context, key string, loader Loader) (Entry, error) {
	if entry, ok, err := c.Get(ctx, key); err != nil {
		return Entry{}, err
	} else if ok {
		return entry, nil
	}
	if c.closed.Load() {
		return Entry{}, ErrClosed
	}

	name := c.key(key)
	if c.negative.has(name) {
		// A recent authoritative "absent": answer without touching the source.
		c.metrics.negativeHits.Add(1)
		return Entry{}, ErrNotFound
	}

	result, err := c.loads.do(ctx, name, func(loadCtx context.Context) (Entry, error) {
		// The configured timeout bounds the load, not the caller's whole request.
		if c.cfg.LoaderTimeout > 0 {
			var cancel context.CancelFunc
			loadCtx, cancel = context.WithTimeout(loadCtx, c.cfg.LoaderTimeout)
			defer cancel()
		}

		value, ttl, err := loader(loadCtx)
		if err != nil {
			c.metrics.loadErrors.Add(1)
			if err == ErrNotFound {
				// Only an explicit not-found is cached; a transient failure is not,
				// because caching it would turn an outage into permanent absence.
				c.negative.add(name, c.cfg.NegativeTTL)
			}
			return Entry{}, err
		}
		c.negative.remove(name)
		if int64(len(value)) > c.cfg.RemoteMaxValueBytes {
			return Entry{}, fmt.Errorf("%w: %d bytes exceed %d", ErrTooLarge, len(value), c.cfg.RemoteMaxValueBytes)
		}

		effective := c.effectiveTTL(ttl)
		if c.remote != nil {
			if err := c.remote.Set(loadCtx, name, value, effective); err != nil {
				return Entry{}, err
			}
		}
		c.metrics.loads.Add(1)
		entry := Entry{Value: value, ExpiresAt: c.clock().Add(effective)}
		c.local.set(name, value, entry.ExpiresAt, 0)
		if err := c.invalidate(loadCtx, name, 0); err != nil {
			// The value is loaded and written; a broadcast failure is reported
			// but must not discard the result.
			var invalid *InvalidationError
			if ok := asInvalidationError(err, &invalid); ok {
				return entry, err
			}
			return entry, err
		}
		return entry, nil
	})
	if err != nil {
		return Entry{}, err
	}
	return result, nil
}

// Invalidate drops one key locally; the subscriber calls it after a broadcast.
func (c *Cache) invalidateLocal(key string, version uint64) {
	name := c.key(key)
	if version == 0 {
		c.local.delete(name)
		return
	}
	c.local.deleteIfVersion(name, version)
}

// ClearLocal empties the local tier (the subscriber does this when its queue
// overflowed).
func (c *Cache) ClearLocal() {
	c.local.clear()
}

// LocalStats snapshots the local tier counters.
func (c *Cache) LocalStats() LocalStats { return c.local.stats() }

// Metrics snapshots every counter.
func (c *Cache) Metrics() Metrics { return c.metrics.snapshot() }

// HealthChecker is implemented by a store that can verify its connection.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Health probes the shared store. A cache with no remote is always healthy.
func (c *Cache) Health(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	checker, ok := c.remote.(HealthChecker)
	if !ok {
		return nil
	}
	if err := checker.Ping(ctx); err != nil {
		c.metrics.healthFailures.Add(1)
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

// NegativeEntries reports how many absence markers are live, which is the only
// way to see that negative caching is doing anything.
func (c *Cache) NegativeEntries() int { return c.negative.size() }

// Namespace reports the configured namespace.
func (c *Cache) Namespace() string { return c.cfg.Namespace }

// key applies the namespace prefix. Business isolation dimensions (tenant, device)
// belong in the key built by the caller.
func (c *Cache) key(key string) string { return gns.Join(":", c.cfg.Namespace, key) }

func (c *Cache) effectiveTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = c.cfg.DefaultTTL
	}
	if ttl > c.cfg.MaxTTL {
		ttl = c.cfg.MaxTTL
	}
	if c.cfg.Jitter > 0 {
		// Deterministic-per-call jitter is enough: entries created at the same
		// moment should not share an expiry.
		delta := int64(float64(ttl) * c.cfg.Jitter)
		if delta > 0 {
			offset := time.Duration((time.Now().UnixNano() % delta) - delta/2)
			ttl += offset
		}
		if ttl <= 0 {
			ttl = c.cfg.DefaultTTL
		}
	}
	return ttl
}

func (c *Cache) invalidate(ctx context.Context, name string, version uint64) error {
	if c.inv == nil {
		return nil
	}
	err := c.inv.Publish(ctx, Invalidation{
		Namespace: c.cfg.Namespace,
		Key:       name,
		Version:   version,
		Origin:    c.origin,
	})
	if err != nil {
		c.metrics.invalidationErrors.Add(1)
		return &InvalidationError{Key: name, Err: err}
	}
	c.metrics.invalidationsPublished.Add(1)
	return nil
}

func (c *Cache) subscribe() {
	defer c.wg.Done()
	_ = c.inv.Subscribe(c.ctx, func(message Invalidation) {
		c.metrics.invalidationsReceived.Add(1)
		if message.Namespace != c.cfg.Namespace {
			return
		}
		if message.Origin == c.origin {
			return // our own write; the local tier is already correct
		}
		c.negative.remove(message.Key)
		if message.Version == 0 {
			c.local.delete(message.Key)
			return
		}
		c.local.deleteIfVersion(message.Key, message.Version)
	})
}

func asInvalidationError(err error, target **InvalidationError) bool {
	if e, ok := err.(*InvalidationError); ok {
		*target = e
		return true
	}
	return false
}
