package gcache

import "context"

// Scanner is an optional capability of a RemoteStore: it enumerates the keys of
// the shared tier that begin with a prefix, so a consumer can find what it
// stored without depending on the store's own command set.
//
// It is deliberately not part of RemoteStore. The two-level cache only ever
// addresses a key it already knows — Get, Set and Delete all take the key from
// the caller — so the cache never enumerates and a store that cannot is still a
// complete RemoteStore. A consumer that needs enumeration (a session store that
// must list the sessions it holds, an operator tool) asks for this interface
// with a type assertion and must handle a store that does not implement it:
//
//	scanner, ok := store.(gcache.Scanner)
//	if !ok {
//		return errors.New("this store cannot enumerate its keys")
//	}
//
// Contract:
//
//   - prefix is a literal key prefix in the store's own key space — the same key
//     string a caller would pass to Get/Set, including any namespace the store
//     adds itself — not a glob or another pattern;
//   - fn is called once per matching key, in an order the implementation does
//     not promise;
//   - the implementation bounds how many keys one round trip asks the store for,
//     so scanning a large tier does not materialise every key in one reply;
//   - an error returned by fn stops the scan and is returned to the caller
//     unchanged, so a caller can abort with its own sentinel;
//   - a store failure is reported as an error wrapping ErrUnavailable, never as
//     an empty result: "the scan broke" and "there is nothing under the prefix"
//     must stay distinguishable, exactly as for Get.
type Scanner interface {
	Scan(ctx context.Context, prefix string, fn func(key string) error) error
}
