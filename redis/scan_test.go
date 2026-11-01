package redis_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/redis"
)

// scanKeys collects one Scan into a slice.
func scanKeys(t *testing.T, store *redis.Store, prefix string) []string {
	t.Helper()
	var got []string
	if err := store.Scan(context.Background(), prefix, func(key string) error {
		got = append(got, key)
		return nil
	}); err != nil {
		t.Fatalf("scan %q: %v", prefix, err)
	}
	return got
}

// Scan returns exactly the keys under the prefix: not a superset (a shorter or
// unrelated key must not come back) and not the keys of another namespace.
func TestScanFiltersByPrefix(t *testing.T) {
	_, client := newClient(t)
	store, err := redis.NewStore(client)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ctx := context.Background()
	seed := []string{"session:1", "session:2", "session:", "other:1", "sessions:3", "sess:4"}
	for _, key := range seed {
		if err := store.Set(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}

	got := scanKeys(t, store, "session:")
	want := []string{"session:", "session:1", "session:2"}
	if len(got) != len(want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("scan = %v, want %v", got, want)
		}
	}

	if got := scanKeys(t, store, "nothing:"); len(got) != 0 {
		t.Fatalf("scan of an unused prefix = %v, want nothing", got)
	}
}

// A key set larger than one page must come back complete. miniredis answers SCAN
// with every match and cursor 0 (it ignores COUNT, which the Redis docs allow),
// so this pins completeness, not the paging loop; TestScanPagesUntilCursorIsZero
// pins the loop itself.
func TestScanLargeKeySet(t *testing.T) {
	_, client := newClient(t)
	store, _ := redis.NewStore(client)
	ctx := context.Background()

	const keys = 600
	for i := 0; i < keys; i++ {
		if err := store.Set(ctx, fmt.Sprintf("large:%03d", i), []byte("v"), time.Minute); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	// Keys outside the prefix must not be counted, even in a large store.
	for i := 0; i < 50; i++ {
		if err := store.Set(ctx, fmt.Sprintf("other:%03d", i), []byte("v"), time.Minute); err != nil {
			t.Fatalf("set other %d: %v", i, err)
		}
	}

	got := scanKeys(t, store, "large:")
	if len(got) != keys {
		t.Fatalf("scan returned %d keys, want %d", len(got), keys)
	}
	seen := make(map[string]struct{}, len(got))
	for _, key := range got {
		seen[key] = struct{}{}
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("large:%03d", i)
		if _, ok := seen[key]; !ok {
			t.Fatalf("scan missed %q", key)
		}
	}
}

// pagedClient answers SCAN like a server that honours COUNT: it returns a fixed
// page and a non-zero cursor until the last one. It also records what the store
// asked for, so the MATCH pattern and the per-call bound are asserted, not assumed.
type pagedClient struct {
	goredis.UniversalClient
	pages   [][]string
	next    []uint64
	calls   int
	cursors []uint64
	matches []string
	counts  []int64
}

func (p *pagedClient) Scan(_ context.Context, cursor uint64, match string, count int64) *goredis.ScanCmd {
	calls := p.calls
	p.calls++
	p.cursors = append(p.cursors, cursor)
	p.matches = append(p.matches, match)
	p.counts = append(p.counts, count)
	if calls >= len(p.pages) {
		return goredis.NewScanCmdResult(nil, 0, nil)
	}
	return goredis.NewScanCmdResult(p.pages[calls], p.next[calls], nil)
}

// The store walks the cursor until the server reports 0, and passes the prefix as
// the MATCH pattern with a bounded COUNT on every call.
func TestScanPagesUntilCursorIsZero(t *testing.T) {
	client := &pagedClient{
		pages: [][]string{
			{"pfx:a", "pfx:b"},
			{"pfx:c"},
			{"pfx:d", "pfx:e"},
		},
		next: []uint64{7, 42, 0},
	}
	store, err := redis.NewStore(client)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	var got []string
	if err := store.Scan(context.Background(), "pfx:", func(key string) error {
		got = append(got, key)
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}

	want := []string{"pfx:a", "pfx:b", "pfx:c", "pfx:d", "pfx:e"}
	if len(got) != len(want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("scan = %v, want %v", got, want)
		}
	}

	if client.calls != 3 {
		t.Fatalf("SCAN called %d times, want 3: the scan stopped early or looped", client.calls)
	}
	for idx, cursor := range client.cursors {
		wantCursor := []uint64{0, 7, 42}[idx]
		if cursor != wantCursor {
			t.Fatalf("call %d sent cursor %d, want %d", idx, cursor, wantCursor)
		}
	}
	for idx, match := range client.matches {
		if match != "pfx:*" {
			t.Fatalf("call %d asked MATCH %q, want %q", idx, match, "pfx:*")
		}
	}
	for idx, count := range client.counts {
		if count <= 0 {
			t.Fatalf("call %d asked COUNT %d: the page is unbounded", idx, count)
		}
		if count != client.counts[0] {
			t.Fatalf("call %d asked COUNT %d, first call asked %d: the bound changes per page", idx, count, client.counts[0])
		}
	}
}

// An error from the callback stops the scan and comes back unchanged, so a caller
// can abort with its own sentinel instead of receiving a wrapped store error.
func TestScanStopsOnCallbackError(t *testing.T) {
	_, client := newClient(t)
	store, _ := redis.NewStore(client)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := store.Set(ctx, fmt.Sprintf("stop:%d", i), []byte("v"), time.Minute); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	stop := errors.New("stop the scan")
	visited := 0
	err := store.Scan(ctx, "stop:", func(string) error {
		visited++
		if visited == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("scan = %v, want the callback's error", err)
	}
	if visited != 3 {
		t.Fatalf("the callback ran %d times, want 3: the scan continued after the error", visited)
	}

	// The store has no memory of the aborted scan: a fresh one sees every key.
	if got := scanKeys(t, store, "stop:"); len(got) != 10 {
		t.Fatalf("a scan after an aborted one returned %d keys, want 10", len(got))
	}
}

// A store failure is ErrUnavailable, never an empty result that would look like
// "there is nothing under the prefix".
func TestScanReportsStoreFailure(t *testing.T) {
	_, client := newClient(t)
	store, _ := redis.NewStore(client)

	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	err := store.Scan(context.Background(), "any:", func(string) error { return nil })
	if !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("scan on a closed client = %v, want ErrUnavailable", err)
	}
}

// A nil callback can only be a programming error: refuse it instead of scanning
// the store for nothing.
func TestScanRejectsNilCallback(t *testing.T) {
	_, client := newClient(t)
	store, _ := redis.NewStore(client)

	if err := store.Scan(context.Background(), "any:", nil); !errors.Is(err, gcache.ErrInvalidConfig) {
		t.Fatalf("scan with a nil callback = %v, want ErrInvalidConfig", err)
	}
}
