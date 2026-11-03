// Package olric implements gcache's shared tier on an embedded Olric cluster.
//
// Olric is the closest Go equivalent of Hazelcast: a clustered distributed map. It
// replaces the Redis L2 when a deployment should not depend on Redis at all; the
// local tier and the invalidation contract are unchanged.
//
// The package is split in two on purpose:
//
//   - DMap, the narrow port gcache needs (plain bytes plus a TTL), which keeps the
//     cache free of Olric types and makes the adapter testable;
//   - FromDMap, the thin bridge to olric.DMap, the only place that knows Olric's
//     response and option types.
//
// The store implements gcache.Scanner, so a consumer that enumerates its keys at
// start-up (gmqx loading the sessions and retained messages of a previous process)
// works the same way over Olric as over Redis. The cost is worth knowing: Olric
// has no prefix index, so a walk visits every partition of the cluster and filters
// each key with a regular expression. Count bounds one page (scanCount) and the
// callback stops the walk early, which fits the job — one pass at start-up, never
// on the request path.
package olric

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	olricdb "github.com/olric-data/olric"

	"github.com/maxid/gcache"
)

// scanCount bounds one scan round trip: Olric calls it the amount of work to do
// per call and defaults to 10. A larger page keeps the number of cluster round
// trips down without asking one partition for an unbounded reply.
const scanCount = 512

// DMap is the subset of a distributed map that a shared tier needs.
type DMap interface {
	// Get returns the value and whether the key exists. A missing key is
	// (nil, false, nil); an outage is an error.
	Get(ctx context.Context, key string) ([]byte, bool, error)
	// Put stores the value for at most ttl.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes the key; deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// Scan calls fn once per key that begins with prefix, in an order the
	// implementation does not promise, and stops when fn returns an error. The
	// key handed to fn is the full key, prefix included.
	//
	// fn's error is returned unchanged, so a caller can stop the walk with its
	// own sentinel. A store failure is an error wrapping gcache.ErrUnavailable —
	// "the walk broke" and "nothing matched" stay distinguishable, exactly as for
	// Get. The walk is paged: the implementation bounds what one round trip asks
	// the store for.
	Scan(ctx context.Context, prefix string, fn func(key string) error) error
}

// Store implements gcache.RemoteStore and gcache.Scanner over a DMap.
//
// The assertion lives here, next to the implementation, so a refactor that drops
// either method fails to compile.
var (
	_ gcache.RemoteStore = (*Store)(nil)
	_ gcache.Scanner     = (*Store)(nil)
)

type Store struct {
	dmap       DMap
	defaultTTL time.Duration
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithDefaultTTL sets the TTL used when a write carries none.
func WithDefaultTTL(ttl time.Duration) StoreOption {
	return func(s *Store) {
		if ttl > 0 {
			s.defaultTTL = ttl
		}
	}
}

// NewStore wraps a DMap.
func NewStore(dmap DMap, opts ...StoreOption) (*Store, error) {
	if dmap == nil {
		return nil, fmt.Errorf("%w: DMap is required", gcache.ErrInvalidConfig)
	}
	s := &Store{dmap: dmap, defaultTTL: time.Hour}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Get implements gcache.RemoteStore.
func (s *Store) Get(ctx context.Context, key string) (gcache.Entry, bool, error) {
	value, ok, err := s.dmap.Get(ctx, key)
	if err != nil {
		return gcache.Entry{}, false, fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	if !ok {
		return gcache.Entry{}, false, nil
	}
	return gcache.Entry{Value: value}, true, nil
}

// Set implements gcache.RemoteStore.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	if err := s.dmap.Put(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}

// Delete implements gcache.RemoteStore.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.dmap.Delete(ctx, key); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}

// Scan implements gcache.Scanner by forwarding to the DMap. Nothing is translated
// here: DMap.Scan already reports a store failure as gcache.ErrUnavailable and a
// callback error unchanged, which is exactly the Scanner contract.
func (s *Store) Scan(ctx context.Context, prefix string, fn func(key string) error) error {
	if fn == nil {
		return fmt.Errorf("%w: scan callback is required", gcache.ErrInvalidConfig)
	}
	return s.dmap.Scan(ctx, prefix, fn)
}

// Close implements gcache.RemoteStore. The Olric node belongs to the caller.
func (s *Store) Close() error { return nil }

// FromDMap adapts a real olric.DMap to the port. This is the only place that
// touches Olric's response, option and error types.
func FromDMap(dmap olricdb.DMap) DMap { return olricBridge{dmap: dmap} }

type olricBridge struct{ dmap olricdb.DMap }

func (b olricBridge) Get(ctx context.Context, key string) ([]byte, bool, error) {
	response, err := b.dmap.Get(ctx, key)
	if err != nil {
		if err == olricdb.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	if response == nil {
		return nil, false, nil
	}
	value, err := response.Byte()
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (b olricBridge) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return b.dmap.Put(ctx, key, value, olricdb.EX(ttl))
}

func (b olricBridge) Delete(ctx context.Context, key string) error {
	_, err := b.dmap.Delete(ctx, key)
	return err
}

// Scan walks the DMap. Olric matches the pattern as a regular expression against
// the whole key and does not anchor it, so the prefix is quoted and anchored here:
// an unanchored "a.b" would also match "axb", and an unquoted "." any character.
// sub-cluster iteration errors surface as Next() returning false, so a walk that
// loses a node ends quietly — the same property as SCAN losing a key to a
// concurrent write, and the caller re-reads what it needs anyway.
func (b olricBridge) Scan(ctx context.Context, prefix string, fn func(key string) error) error {
	iterator, err := b.dmap.Scan(ctx, olricdb.Match("^"+regexp.QuoteMeta(prefix)), olricdb.Count(scanCount))
	if err != nil {
		// An empty DMap yields an iterator that returns nothing, but the
		// command layer of some Olric versions answers ErrKeyNotFound instead:
		// both mean "nothing under this prefix".
		if errors.Is(err, olricdb.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	defer iterator.Close()

	for iterator.Next() {
		key := iterator.Key()
		// The pattern is anchored and quoted, so this only guards against a
		// future change in how Olric applies Match.
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}
