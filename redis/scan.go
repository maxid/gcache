package redis

import (
	"context"
	"fmt"

	"github.com/maxid/gcache"
)

// scanCount is the COUNT of one SCAN round trip: a page size, not a limit on how
// many keys a scan returns. It bounds the reply of one call so scanning a large
// tier does not build the whole key list in one response.
const scanCount = 512

// Store implements gcache.Scanner. The assertion lives here, next to the
// implementation, so a refactor that drops the method fails to compile.
var _ gcache.Scanner = (*Store)(nil)

// Scan implements gcache.Scanner with SCAN + MATCH <prefix>* + COUNT.
//
// SCAN is used instead of KEYS because KEYS is O(N) and blocks the server while
// it walks the whole database: a scan of a shared store is an operator or
// background operation and must not stop the cache. The price is SCAN's normal
// contract — a key present for the whole scan is returned at least once, but
// duplicates are possible, keys added or removed during the scan may or may not
// appear, and the order is unspecified. A caller that needs a stable set reads
// the keys it is given and tolerates those properties.
//
// prefix is used verbatim in the MATCH pattern, so a prefix containing a glob
// metacharacter (`*`, `?`, `[`, `]`, `\`) can match more keys than the caller
// meant. Key prefixes in this ecosystem are namespace-like segments; a caller
// that builds one from untrusted input must keep those characters out of it.
func (s *Store) Scan(ctx context.Context, prefix string, fn func(key string) error) error {
	if fn == nil {
		return fmt.Errorf("%w: scan callback is required", gcache.ErrInvalidConfig)
	}

	pattern := prefix + "*"
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, scanCount).Result()
		if err != nil {
			return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
		}
		for _, key := range keys {
			if err := fn(key); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
