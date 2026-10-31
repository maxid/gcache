package olric_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxid/gcache"
	golric "github.com/maxid/gcache/olric"
)

// fakeDMap records what the store asks for; the real Olric node is a deployment
// concern, the port contract is what this covers.
type fakeDMap struct {
	values map[string][]byte
	ttls   map[string]time.Duration
	getErr error
	putErr error
	delErr error
}

func newFake() *fakeDMap {
	return &fakeDMap{values: map[string][]byte{}, ttls: map[string]time.Duration{}}
}

func (f *fakeDMap) Get(_ context.Context, key string) ([]byte, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	value, ok := f.values[key]
	return value, ok, nil
}

func (f *fakeDMap) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.values[key] = value
	f.ttls[key] = ttl
	return nil
}

func (f *fakeDMap) Delete(_ context.Context, key string) error {
	if f.delErr != nil {
		return f.delErr
	}
	delete(f.values, key)
	return nil
}

func TestStoreRoundTrip(t *testing.T) {
	dmap := newFake()
	store, err := golric.NewStore(dmap)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ctx := context.Background()

	if _, ok, err := store.Get(ctx, "missing"); ok || err != nil {
		t.Fatalf("missing = %v, %v (absence is not an error)", ok, err)
	}
	if err := store.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if dmap.ttls["k"] != time.Minute {
		t.Fatalf("ttl = %s, want 1m: Olric has no server-side default", dmap.ttls["k"])
	}
	entry, ok, err := store.Get(ctx, "k")
	if err != nil || !ok || string(entry.Value) != "v" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, "k"); ok {
		t.Fatal("entry survived delete")
	}
}

func TestStoreAppliesDefaultTTL(t *testing.T) {
	dmap := newFake()
	store, _ := golric.NewStore(dmap, golric.WithDefaultTTL(30*time.Second))

	if err := store.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	if dmap.ttls["k"] != 30*time.Second {
		t.Fatalf("ttl = %s, want 30s", dmap.ttls["k"])
	}
}

// A failing store must be reported as unavailable, never as "absent": a caller
// that mistakes an outage for a miss will hammer the database.
func TestStoreMapsFailuresToUnavailable(t *testing.T) {
	ctx := context.Background()

	failGet := newFake()
	failGet.getErr = errors.New("cluster unreachable")
	store, _ := golric.NewStore(failGet)
	if _, _, err := store.Get(ctx, "k"); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("get err = %v, want ErrUnavailable", err)
	}

	failPut := newFake()
	failPut.putErr = errors.New("write rejected")
	store, _ = golric.NewStore(failPut)
	if err := store.Set(ctx, "k", []byte("v"), time.Minute); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("set err = %v, want ErrUnavailable", err)
	}

	failDel := newFake()
	failDel.delErr = errors.New("write rejected")
	store, _ = golric.NewStore(failDel)
	if err := store.Delete(ctx, "k"); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("delete err = %v, want ErrUnavailable", err)
	}
}

func TestNewStoreValidates(t *testing.T) {
	if _, err := golric.NewStore(nil); !errors.Is(err, gcache.ErrInvalidConfig) {
		t.Fatalf("nil dmap err = %v", err)
	}
}

// The cache works over the Olric port exactly as it does over Redis.
func TestCacheOverOlric(t *testing.T) {
	store, _ := golric.NewStore(newFake())
	cache, err := gcache.New(
		gcache.Config{Namespace: "test"},
		gcache.WithRemote(store),
	)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	if err := cache.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if entry, ok, err := cache.Get(ctx, "k"); err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}

	cache.ClearLocal() // force a read through the Olric tier
	if entry, ok, err := cache.Get(ctx, "k"); err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get after clear = %+v, %v, %v", entry, ok, err)
	}
}
