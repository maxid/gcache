package gcache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/memory"
)

// clock is a controllable time source for the TTL and stale tests.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newClockCache(t *testing.T, cfg gcache.Config, opts ...gcache.Option) (*gcache.Cache, *clock) {
	t.Helper()
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	cache, err := gcache.New(cfg, append([]gcache.Option{gcache.WithClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache, clk
}

// The negative marker used to be written to the shared store and never read, so
// a hot missing key still hit the loader on every call. It is now consulted.
func TestNegativeCacheSuppressesRepeatedLoads(t *testing.T) {
	cache, _ := newClockCache(t, gcache.Config{Namespace: "test", NegativeTTL: time.Minute})

	var calls atomic.Int64
	loader := func(context.Context) ([]byte, time.Duration, error) {
		calls.Add(1)
		return nil, 0, gcache.ErrNotFound
	}

	for i := 0; i < 3; i++ {
		if _, err := cache.GetOrLoad(context.Background(), "missing", loader); !errors.Is(err, gcache.ErrNotFound) {
			t.Fatalf("call %d: err = %v", i, err)
		}
	}

	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1 (negative cache)", calls.Load())
	}
	if metrics := cache.Metrics(); metrics.NegativeHits != 2 {
		t.Fatalf("negative hits = %d, want 2", metrics.NegativeHits)
	}
	if cache.NegativeEntries() != 1 {
		t.Fatalf("markers = %d, want 1", cache.NegativeEntries())
	}
}

func TestNegativeCacheExpires(t *testing.T) {
	cache, clk := newClockCache(t, gcache.Config{Namespace: "test", NegativeTTL: time.Minute})

	var calls atomic.Int64
	loader := func(context.Context) ([]byte, time.Duration, error) {
		calls.Add(1)
		return nil, 0, gcache.ErrNotFound
	}

	_, _ = cache.GetOrLoad(context.Background(), "missing", loader)
	clk.Advance(2 * time.Minute)
	_, _ = cache.GetOrLoad(context.Background(), "missing", loader)

	if calls.Load() != 2 {
		t.Fatalf("loader ran %d times, want 2 after the marker expired", calls.Load())
	}
}

// Storing a value after a miss must clear the marker, otherwise the new value is
// shadowed by the old absence.
func TestWriteClearsTheNegativeMarker(t *testing.T) {
	cache, _ := newClockCache(t, gcache.Config{Namespace: "test", NegativeTTL: time.Minute})
	ctx := context.Background()

	if _, err := cache.GetOrLoad(ctx, "k", func(context.Context) ([]byte, time.Duration, error) {
		return nil, 0, gcache.ErrNotFound
	}); !errors.Is(err, gcache.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}

	if err := cache.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if cache.NegativeEntries() != 0 {
		t.Fatal("marker survived a write")
	}
	if _, err := cache.GetOrLoad(ctx, "k", func(context.Context) ([]byte, time.Duration, error) {
		return nil, 0, gcache.ErrNotFound
	}); err != nil {
		t.Fatalf("get after write = %v", err)
	}
}

// The configured loader timeout must bound the load, not leak into the caller.
func TestLoaderTimeoutIsEnforced(t *testing.T) {
	cache, _ := newClockCache(t, gcache.Config{Namespace: "test", LoaderTimeout: 50 * time.Millisecond})

	start := time.Now()
	_, err := cache.GetOrLoad(context.Background(), "slow", func(ctx context.Context) ([]byte, time.Duration, error) {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(5 * time.Second):
			return []byte("late"), time.Minute, nil
		}
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("slow loader must fail through the timeout")
	}
	if elapsed > time.Second {
		t.Fatalf("load took %s, want it bounded by the 50ms timeout", elapsed)
	}
}

// A background sweep reclaims expired entries so they stop occupying the budget.
func TestBackgroundSweepReclaimsExpiredEntries(t *testing.T) {
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	cache, err := gcache.New(
		gcache.Config{Namespace: "test", SweepInterval: 100 * time.Millisecond},
		gcache.WithClock(clk.Now),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cache.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := cache.Set(ctx, string(rune('a'+i)), []byte("v"), time.Second); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if stats := cache.LocalStats(); stats.Entries != 5 {
		t.Fatalf("entries = %d, want 5", stats.Entries)
	}

	clk.Advance(2 * time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if cache.LocalStats().Entries == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sweep did not run: %d entries left", cache.LocalStats().Entries)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stale-if-error: an expired local entry is served only when the store fails, and
// the result is marked stale so the caller can tell.
func TestStaleIfErrorServesExpiredEntryOnStoreFailure(t *testing.T) {
	store := &failingStore{Store: memory.NewStore()}
	cache, clk := newClockCache(t,
		gcache.Config{Namespace: "test", StaleIfError: time.Minute},
		gcache.WithRemote(store),
	)
	ctx := context.Background()

	if err := cache.Set(ctx, "k", []byte("v1"), time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	store.fail.Store(true)
	clk.Advance(2 * time.Second) // past TTL, inside the stale window

	entry, ok, err := cache.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || string(entry.Value) != "v1" {
		t.Fatalf("entry = %+v, %v", entry, ok)
	}
	if !entry.Stale {
		t.Fatal("a value served past its TTL must be marked stale")
	}
	if metrics := cache.Metrics(); metrics.StaleServed != 1 {
		t.Fatalf("stale served = %d, want 1", metrics.StaleServed)
	}
}

// Without the stale window, a failed store is an error — not a silent stale read.
func TestNoStaleWhenNotConfigured(t *testing.T) {
	store := &failingStore{Store: memory.NewStore()}
	cache, clk := newClockCache(t, gcache.Config{Namespace: "test"}, gcache.WithRemote(store))
	ctx := context.Background()

	if err := cache.Set(ctx, "k", []byte("v1"), time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	store.fail.Store(true)
	clk.Advance(2 * time.Second)

	if _, _, err := cache.Get(ctx, "k"); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestHealthReportsTheStore(t *testing.T) {
	store := &failingStore{Store: memory.NewStore()}
	cache, _ := newClockCache(t, gcache.Config{Namespace: "test"}, gcache.WithRemote(store))
	ctx := context.Background()

	if err := cache.Health(ctx); err != nil {
		t.Fatalf("healthy store reported %v", err)
	}
	store.fail.Store(true)
	if err := cache.Health(ctx); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if metrics := cache.Metrics(); metrics.HealthFailures != 1 {
		t.Fatalf("health failures = %d, want 1", metrics.HealthFailures)
	}

	// A cache with no remote is always healthy.
	local, _ := newClockCache(t, gcache.Config{Namespace: "local"})
	if err := local.Health(ctx); err != nil {
		t.Fatalf("local cache health = %v", err)
	}
}

// failingStore is a RemoteStore whose failure can be switched on.
type failingStore struct {
	*memory.Store
	fail atomic.Bool
}

func (f *failingStore) Get(ctx context.Context, key string) (gcache.Entry, bool, error) {
	if f.fail.Load() {
		return gcache.Entry{}, false, gcache.ErrUnavailable
	}
	return f.Store.Get(ctx, key)
}

func (f *failingStore) Ping(context.Context) error {
	if f.fail.Load() {
		return gcache.ErrUnavailable
	}
	return nil
}
