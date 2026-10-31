//go:build integration

package olric_test

import (
	"context"
	"net"
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
