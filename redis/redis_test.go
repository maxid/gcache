package redis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/redis"
)

func newClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func TestStoreRoundTrip(t *testing.T) {
	_, client := newClient(t)
	store, err := redis.NewStore(client)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ctx := context.Background()

	if _, ok, err := store.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing = %v, %v", ok, err)
	}
	if err := store.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry, ok, err := store.Get(ctx, "k")
	if err != nil || !ok || string(entry.Value) != "v" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}
	if entry.ExpiresAt.IsZero() {
		t.Fatal("expiry not reported: a local tier would cache it forever")
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, "k"); ok {
		t.Fatal("entry survived delete")
	}
}

func TestInvalidatorRoundTrip(t *testing.T) {
	_, client := newClient(t)
	inv, err := redis.NewInvalidator(client, "gcache:test")
	if err != nil {
		t.Fatalf("invalidator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	received := make(chan gcache.Invalidation, 1)
	go func() {
		_ = inv.Subscribe(ctx, func(message gcache.Invalidation) {
			select {
			case received <- message:
			default:
			}
		})
	}()

	// Give the subscriber a moment to attach; pub/sub has no subscription ack
	// exposed here.
	time.Sleep(100 * time.Millisecond)
	if err := inv.Publish(ctx, gcache.Invalidation{Namespace: "test", Key: "test:k", Version: 3}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case message := <-received:
		if message.Key != "test:k" || message.Version != 3 {
			t.Fatalf("message = %+v", message)
		}
	case <-ctx.Done():
		t.Fatal("no invalidation received")
	}
}

// miniredis implements EVAL but not DELEX, so the locker must detect that and fall
// back to the Lua scripts — the same path an older production Redis takes.
func TestLockerFallsBackToScripts(t *testing.T) {
	_, client := newClient(t)
	locker, err := redis.NewLocker(client, "gcache:fencing")
	if err != nil {
		t.Fatalf("locker: %v", err)
	}
	ctx := context.Background()

	lock, ok, err := locker.TryAcquire(ctx, "job:1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	if locker.UsesNativeConditionals() {
		t.Fatal("locker claimed native conditionals on a server without DELEX")
	}
	if lock.FencingToken() == 0 {
		t.Fatal("fencing token not allocated")
	}

	// A second locker cannot take the same key.
	other, _ := redis.NewLocker(client, "gcache:fencing")
	if _, ok, err := other.TryAcquire(ctx, "job:1", time.Minute); err != nil || ok {
		t.Fatalf("second acquire = %v, %v (want held)", ok, err)
	}

	if err := lock.Renew(ctx, 2*time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := lock.Unlock(ctx); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("second unlock = %v, want ErrLockLost", err)
	}

	// Released: it can be taken again.
	if _, ok, err := other.TryAcquire(ctx, "job:1", time.Minute); err != nil || !ok {
		t.Fatalf("reacquire = %v, %v", ok, err)
	}
}

// A lock that expired is no longer ours; renewing and unlocking must say so
// instead of pretending success.
func TestLockerDetectsLostLock(t *testing.T) {
	server, client := newClient(t)
	locker, _ := redis.NewLocker(client, "gcache:fencing")
	ctx := context.Background()

	lock, ok, err := locker.TryAcquire(ctx, "job:2", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}

	// Expire the key behind our back, then let another owner take it.
	server.Del("job:2")
	other, _ := redis.NewLocker(client, "gcache:fencing")
	if _, ok, _ := other.TryAcquire(ctx, "job:2", time.Minute); !ok {
		t.Fatal("other locker could not take the released key")
	}

	if err := lock.Renew(ctx, time.Minute); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("renew = %v, want ErrLockLost", err)
	}
	if err := lock.Unlock(ctx); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("unlock = %v, want ErrLockLost", err)
	}
}

func TestLockerAcquireWaitsThenSucceeds(t *testing.T) {
	server, client := newClient(t)
	locker, _ := redis.NewLocker(client, "gcache:fencing")
	ctx := context.Background()

	first, ok, err := locker.TryAcquire(ctx, "job:3", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first = %v, %v", ok, err)
	}

	released := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		server.Del("job:3")
		close(released)
	}()

	wait := gcache.WaitPolicy{Timeout: time.Second, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	second, err := locker.Acquire(ctx, "job:3", time.Minute, wait)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	<-released
	if err := second.Unlock(ctx); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	_ = first
}

func TestLockerAcquireTimesOut(t *testing.T) {
	_, client := newClient(t)
	locker, _ := redis.NewLocker(client, "gcache:fencing")
	ctx := context.Background()

	if _, ok, err := locker.TryAcquire(ctx, "job:4", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}

	wait := gcache.WaitPolicy{Timeout: 50 * time.Millisecond, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
	if _, err := locker.Acquire(ctx, "job:4", time.Minute, wait); !errors.Is(err, gcache.ErrLockNotAcquired) {
		t.Fatalf("err = %v, want ErrLockNotAcquired", err)
	}
}

// The cache works end to end on Redis: L1 serves locally, L2 restores after a
// local clear, and the broadcast reaches another node.
func TestCacheOverRedis(t *testing.T) {
	_, client := newClient(t)
	store, _ := redis.NewStore(client)
	inv, _ := redis.NewInvalidator(client, "gcache:cache")

	nodeA, err := gcache.New(
		gcache.Config{Namespace: "test"},
		gcache.WithRemote(store),
		gcache.WithInvalidator(inv),
		gcache.WithOrigin("a"),
	)
	if err != nil {
		t.Fatalf("cache a: %v", err)
	}
	defer nodeA.Close()

	// A second node with its own local tier and the same Redis + channel.
	nodeB, err := gcache.New(
		gcache.Config{Namespace: "test"},
		gcache.WithRemote(store),
		gcache.WithInvalidator(inv),
		gcache.WithOrigin("b"),
	)
	if err != nil {
		t.Fatalf("cache b: %v", err)
	}
	defer nodeB.Close()

	ctx := context.Background()
	if err := nodeA.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	// B reads through L2 and caches locally.
	if entry, ok, err := nodeB.Get(ctx, "k"); err != nil || !ok || string(entry.Value) != "v1" {
		t.Fatalf("b get = %+v, %v, %v", entry, ok, err)
	}

	// A updates; B must not keep serving the old local copy indefinitely.
	if err := nodeA.Set(ctx, "k", []byte("v2"), time.Minute); err != nil {
		t.Fatalf("update: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		entry, ok, err := nodeB.Get(ctx, "k")
		if err != nil {
			t.Fatalf("b get: %v", err)
		}
		if ok && string(entry.Value) == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("b still serves %q after the invalidation", entry.Value)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestConcurrentLockingOnlyOneWins(t *testing.T) {
	_, client := newClient(t)
	locker, _ := redis.NewLocker(client, "gcache:fencing")

	const workers = 8
	var wins sync.Map
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, ok, err := locker.TryAcquire(context.Background(), "job:race", time.Minute)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if ok {
				wins.Store(id, true)
			}
		}(i)
	}
	wg.Wait()

	count := 0
	wins.Range(func(any, any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("%d workers acquired the same lock, want 1", count)
	}
}
