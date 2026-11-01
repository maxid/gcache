package gcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/memory"
)

// keyOnlyStore is a complete RemoteStore that does not enumerate: it is what the
// port looks like without the optional capability.
type keyOnlyStore struct {
	values map[string]gcache.Entry
}

func (s *keyOnlyStore) Get(_ context.Context, key string) (gcache.Entry, bool, error) {
	entry, ok := s.values[key]
	return entry, ok, nil
}

func (s *keyOnlyStore) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if s.values == nil {
		s.values = map[string]gcache.Entry{}
	}
	s.values[key] = gcache.Entry{Value: value, ExpiresAt: time.Now().Add(ttl)}
	return nil
}

func (s *keyOnlyStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func (s *keyOnlyStore) Close() error { return nil }

// scannerOf is how a consumer asks for the optional capability: a type assertion
// that simply fails on a store without it. The cache itself never scans, so a
// store that does not implement Scanner is still a valid RemoteStore and the
// cache built on it works.
func scannerOf(store gcache.RemoteStore) (gcache.Scanner, bool) {
	scanner, ok := store.(gcache.Scanner)
	return scanner, ok
}

func TestScannerIsOptional(t *testing.T) {
	var store gcache.RemoteStore = &keyOnlyStore{}
	if _, ok := scannerOf(store); ok {
		t.Fatal("a key-addressed store claimed the Scanner capability")
	}

	// The same assertion on the in-process store: it does not enumerate either.
	if _, ok := scannerOf(memory.NewStore()); ok {
		t.Fatal("memory.Store claimed the Scanner capability")
	}

	// A cache over a store without the capability still works: enumeration is not
	// a caching operation.
	cache, err := gcache.New(gcache.Config{Namespace: "test"}, gcache.WithRemote(store))
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	if err := cache.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if entry, ok, err := cache.Get(ctx, "k"); err != nil || !ok || string(entry.Value) != "v" {
		t.Fatalf("get = %+v, %v, %v", entry, ok, err)
	}
}
