//go:build integration

package olric_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"

	"github.com/maxid/gcache"
	golric "github.com/maxid/gcache/olric"
)

// freePort reserves a port and releases it, so the test does not collide with a
// running node. Olric ignores BindPort=0, hence the explicit choice.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// startOlric runs one embedded node and waits until it reports readiness.
func startOlric(t *testing.T) (*olric.Olric, *olric.EmbeddedClient) {
	t.Helper()

	cfg := config.New("local")
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = freePort(t)
	cfg.MemberlistConfig.BindAddr = "127.0.0.1"
	cfg.MemberlistConfig.BindPort = freePort(t)
	cfg.LogLevel = "ERROR"

	started := make(chan struct{})
	cfg.Started = func() { close(started) }

	db, err := olric.New(cfg)
	if err != nil {
		t.Fatalf("olric new: %v", err)
	}

	// Start blocks until the node shuts down, so it runs in its own goroutine.
	startErr := make(chan error, 1)
	go func() { startErr <- db.Start() }()

	select {
	case <-started:
	case err := <-startErr:
		t.Fatalf("olric start: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("olric node never reported readiness")
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.Shutdown(ctx)
	})

	return db, db.NewEmbeddedClient()
}

// The adapter against a real node: this is what the fake could not prove.
func TestStoreAgainstEmbeddedOlric(t *testing.T) {
	_, client := startOlric(t)

	dmap, err := client.NewDMap("gcache")
	if err != nil {
		t.Fatalf("dmap: %v", err)
	}
	store, err := golric.NewStore(golric.FromDMap(dmap))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, ok, err := store.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing = %v, %v (absence is not an error)", ok, err)
	}

	if err := store.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry, ok, err := store.Get(ctx, "k")
	if err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}

	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, "k"); ok {
		t.Fatal("entry survived delete")
	}
}

// Scan against a real node: four keys, two of them under the prefix. The other two
// are the two ways the pattern can over-match, because Olric applies Match as an
// unanchored regular expression — "sessa:3" matches a pattern whose dot is not
// quoted, "xsess.a:4" matches one that is not anchored at the start.
func TestScanAgainstEmbeddedOlric(t *testing.T) {
	_, client := startOlric(t)

	dmap, err := client.NewDMap("gcache-scan")
	if err != nil {
		t.Fatalf("dmap: %v", err)
	}
	store, err := golric.NewStore(golric.FromDMap(dmap))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, key := range []string{"sess.a:1", "sess.a:2", "sessa:3", "xsess.a:4"} {
		if err := store.Set(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	var got []string
	if err := store.Scan(ctx, "sess.a:", func(key string) error {
		got = append(got, key)
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	slices.Sort(got)
	if want := []string{"sess.a:1", "sess.a:2"}; !slices.Equal(got, want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}

	// A prefix nothing matches is an empty walk, not an error: the caller must be
	// able to tell "nothing stored" from "the store is down".
	if err := store.Scan(ctx, "absent:", func(key string) error {
		t.Fatalf("unexpected key %q", key)
		return nil
	}); err != nil {
		t.Fatalf("empty scan: %v", err)
	}

	stop := errors.New("stop")
	if err := store.Scan(ctx, "sess.a:", func(string) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("scan err = %v, want the callback's error unchanged", err)
	}
}

// The two-level cache over a real Olric cluster: L1 answers, L2 restores after a
// local clear.
func TestCacheOverEmbeddedOlric(t *testing.T) {
	_, client := startOlric(t)

	dmap, err := client.NewDMap("gcache-cache")
	if err != nil {
		t.Fatalf("dmap: %v", err)
	}
	store, _ := golric.NewStore(golric.FromDMap(dmap))

	cache, err := gcache.New(gcache.Config{Namespace: "test"}, gcache.WithRemote(store))
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	defer cache.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := cache.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if entry, ok, err := cache.Get(ctx, "k"); err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}

	cache.ClearLocal() // force the Olric tier to answer
	entry, ok, err := cache.Get(ctx, "k")
	if err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("get after clear = %+v, %v, %v", entry, ok, err)
	}
	if metrics := cache.Metrics(); metrics.RemoteHits == 0 {
		t.Fatalf("no remote hit recorded: %+v", metrics)
	}

	if err := cache.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
}

// The lock against a real node, through two clients of the same cluster: exclusion
// has to hold across processes, and a handle whose lease elapsed must report a lost
// lock instead of releasing the new holder's key.
func TestLockerAgainstEmbeddedOlric(t *testing.T) {
	db, first := startOlric(t)
	second := db.NewEmbeddedClient()

	dmaps := make([]olric.DMap, 0, 2)
	for _, client := range []*olric.EmbeddedClient{first, second} {
		dm, err := client.NewDMap("gcache-lock")
		if err != nil {
			t.Fatalf("dmap: %v", err)
		}
		dmaps = append(dmaps, dm)
	}

	lockers := make([]*golric.Locker, 0, 2)
	for _, dm := range dmaps {
		locker, err := golric.NewLocker(golric.FromLockingDMap(dm), "gcache:test-fencing")
		if err != nil {
			t.Fatalf("locker: %v", err)
		}
		lockers = append(lockers, locker)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	held, ok, err := lockers[0].TryAcquire(ctx, "job:1", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire on the first client = %v, %v", ok, err)
	}
	if _, ok, err := lockers[1].TryAcquire(ctx, "job:1", 5*time.Second); ok || err != nil {
		t.Fatalf("acquire on the second client = %v, %v, want a refusal", ok, err)
	}
	if err := held.Unlock(ctx); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	next, ok, err := lockers[1].TryAcquire(ctx, "job:1", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire after release = %v, %v", ok, err)
	}
	if next.FencingToken() <= held.FencingToken() {
		t.Fatalf("fencing %d after %d, want strictly increasing", next.FencingToken(), held.FencingToken())
	}
	if err := next.Unlock(ctx); err != nil {
		t.Fatalf("unlock second: %v", err)
	}

	// A lease that elapses: the key goes away with its TTL, another client takes it,
	// and the old handle must not be able to renew or release it.
	expiring, ok, err := lockers[0].TryAcquire(ctx, "job:2", 150*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("acquire expiring = %v, %v", ok, err)
	}
	time.Sleep(500 * time.Millisecond)
	taken, ok, err := lockers[1].TryAcquire(ctx, "job:2", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire after the lease elapsed = %v, %v", ok, err)
	}
	if err := expiring.Renew(ctx, time.Minute); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("renew of an expired lock = %v, want ErrLockLost", err)
	}
	if err := expiring.Unlock(ctx); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("unlock of an expired lock = %v, want ErrLockLost", err)
	}
	if err := taken.Unlock(ctx); err != nil {
		t.Fatalf("unlock the new holder: %v", err)
	}
}
