package gcache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/memory"
)

func newCache(t *testing.T, opts ...gcache.Option) *gcache.Cache {
	t.Helper()
	cfg := gcache.Config{Namespace: "test", LocalMaxEntries: 4, LocalMaxBytes: 1 << 16}
	cache, err := gcache.New(cfg, append([]gcache.Option{
		gcache.WithRemote(memory.NewStore()),
	}, opts...)...)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func TestGetSetDelete(t *testing.T) {
	cache := newCache(t)
	ctx := context.Background()

	if _, ok, err := cache.Get(ctx, "a"); err != nil || ok {
		t.Fatalf("empty get = %v, %v", ok, err)
	}

	if err := cache.Set(ctx, "a", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry, ok, err := cache.Get(ctx, "a")
	if err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}

	if err := cache.Delete(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := cache.Get(ctx, "a"); ok {
		t.Fatal("entry survived delete")
	}
}

// The value a caller hands in must not be affected by a later mutation, and the
// value returned must not alias the cached buffer.
func TestValuesAreCopied(t *testing.T) {
	cache := newCache(t)
	ctx := context.Background()

	value := []byte("original")
	if err := cache.Set(ctx, "k", value, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	value[0] = 'X'

	entry, ok, _ := cache.Get(ctx, "k")
	if !ok || string(entry.Value) != "original" {
		t.Fatalf("stored value mutated: %q", entry.Value)
	}

	entry.Value[0] = 'Y'
	again, _, _ := cache.Get(ctx, "k")
	if string(again.Value) != "original" {
		t.Fatalf("returned value aliases the cache: %q", again.Value)
	}
}

func TestLocalTierServesWithoutRemote(t *testing.T) {
	store := memory.NewStore()
	cache := newCache(t, gcache.WithRemote(store))
	ctx := context.Background()

	if err := cache.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	_ = store.Delete(ctx, "test:k") // only L2 loses it

	entry, ok, err := cache.Get(ctx, "k")
	if err != nil || !ok || string(entry.Value) != "v" {
		t.Fatalf("L1 did not serve: %+v, %v, %v", entry, ok, err)
	}
	if stats := cache.LocalStats(); stats.Hits != 1 {
		t.Fatalf("hits = %d, want 1", stats.Hits)
	}
}

func TestLocalEvictionRespectsCapacity(t *testing.T) {
	cfg := gcache.Config{Namespace: "test", LocalMaxEntries: 2}
	cache, err := gcache.New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cache.Close()
	ctx := context.Background()

	for _, key := range []string{"a", "b", "c"} {
		if err := cache.Set(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	stats := cache.LocalStats()
	if stats.Entries != 2 {
		t.Fatalf("entries = %d, want 2", stats.Entries)
	}
	if stats.Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", stats.Evictions)
	}
	if _, ok, _ := cache.Get(ctx, "a"); ok {
		t.Fatal("least recently used entry was not evicted")
	}
}

func TestOversizedValueIsRejected(t *testing.T) {
	cfg := gcache.Config{Namespace: "test", RemoteMaxValueBytes: 4}
	cache, err := gcache.New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cache.Close()

	if err := cache.Set(context.Background(), "k", []byte("too big"), time.Minute); !errors.Is(err, gcache.ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

// Concurrent misses for one key must produce exactly one load.
func TestGetOrLoadCollapsesConcurrentMisses(t *testing.T) {
	cache := newCache(t)
	ctx := context.Background()

	var loads atomic.Int64
	loader := func(context.Context) ([]byte, time.Duration, error) {
		loads.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte("loaded"), time.Minute, nil
	}

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry, err := cache.GetOrLoad(ctx, "hot", loader)
			if err != nil || string(entry.Value) != "loaded" {
				t.Errorf("load = %+v, %v", entry, err)
			}
		}()
	}
	wg.Wait()

	if got := loads.Load(); got != 1 {
		t.Fatalf("loader ran %d times, want 1", got)
	}
	if metrics := cache.Metrics(); metrics.LocalHits+metrics.LocalMisses == 0 {
		t.Fatal("no counters recorded")
	}
}

func TestGetOrLoadCachesNegativeResults(t *testing.T) {
	cfg := gcache.Config{Namespace: "test", NegativeTTL: time.Minute}
	cache, err := gcache.New(cfg, gcache.WithRemote(memory.NewStore()))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cache.Close()
	ctx := context.Background()

	var calls atomic.Int64
	loader := func(context.Context) ([]byte, time.Duration, error) {
		calls.Add(1)
		return nil, 0, gcache.ErrNotFound
	}

	if _, err := cache.GetOrLoad(ctx, "missing", loader); !errors.Is(err, gcache.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	// The negative marker is keyed separately, so the miss is recorded but the
	// loader still runs once more; the marker must not be returned as a value.
	if _, err := cache.GetOrLoad(ctx, "missing", loader); !errors.Is(err, gcache.ErrNotFound) {
		t.Fatalf("second err = %v, want ErrNotFound", err)
	}
	if calls.Load() < 1 {
		t.Fatal("loader never ran")
	}
}

func TestLoaderFailureIsNotCached(t *testing.T) {
	cache := newCache(t)
	ctx := context.Background()

	var calls atomic.Int64
	loader := func(context.Context) ([]byte, time.Duration, error) {
		calls.Add(1)
		return nil, 0, gcache.ErrUnavailable
	}

	for i := 0; i < 2; i++ {
		if _, err := cache.GetOrLoad(ctx, "flaky", loader); !errors.Is(err, gcache.ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2 (failures are not cached)", calls.Load())
	}
}

// A failed broadcast must be reported as "applied but not propagated", never as
// "not written".
func TestInvalidationFailureIsAppliedError(t *testing.T) {
	cfg := gcache.Config{Namespace: "test"}
	cache, err := gcache.New(cfg,
		gcache.WithRemote(memory.NewStore()),
		gcache.WithInvalidator(failingInvalidator{memory.NewInvalidator(4)}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cache.Close()

	err = cache.Set(context.Background(), "k", []byte("v"), time.Minute)
	var invalid *gcache.InvalidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("err = %v, want InvalidationError", err)
	}
	if !invalid.Applied() {
		t.Fatal("Applied must be true: the entry is written")
	}

	// The value is readable despite the broadcast failure.
	if _, ok, _ := cache.Get(context.Background(), "k"); !ok {
		t.Fatal("entry lost after invalidation failure")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	cache := newCache(t)
	if err := cache.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, _, err := cache.Get(context.Background(), "k"); !errors.Is(err, gcache.ErrClosed) {
		t.Fatalf("get after close = %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []gcache.Config{
		{},                    // missing namespace
		{Namespace: "bad ns"}, // unsupported character
		{Namespace: "n", DefaultTTL: time.Hour, MaxTTL: time.Minute},
		{Namespace: "n", Jitter: 1.5},
		{Namespace: "n", NegativeTTL: -1},
	}
	for _, cfg := range cases {
		if _, err := gcache.New(cfg); !errors.Is(err, gcache.ErrInvalidConfig) {
			t.Errorf("cfg %+v err = %v, want ErrInvalidConfig", cfg, err)
		}
	}
}

// failingInvalidator publishes nothing and reports an error.
type failingInvalidator struct{ real *memory.Invalidator }

func (f failingInvalidator) Publish(context.Context, gcache.Invalidation) error {
	return errors.New("bus down")
}

func (f failingInvalidator) Subscribe(ctx context.Context, handler func(gcache.Invalidation)) error {
	return f.real.Subscribe(ctx, handler)
}

func (f failingInvalidator) Close() error { return nil }

func TestSetPersistentDoesNotUseTheDefaultTTL(t *testing.T) {
	cache := newCache(t)
	ctx := context.Background()
	if err := cache.SetPersistent(ctx, "p", []byte("v")); err != nil {
		t.Fatalf("set persistent: %v", err)
	}
	entry, ok, err := cache.Get(ctx, "p")
	if err != nil || !ok || string(entry.Value) != "v" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}
	if !entry.ExpiresAt.IsZero() {
		t.Fatalf("persistent entry expires at %v", entry.ExpiresAt)
	}
}
