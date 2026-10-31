package gcache

import "sync/atomic"

// LocalStats is a snapshot of the in-process tier.
type LocalStats struct {
	Entries     int
	Bytes       int64
	Hits        int64
	Misses      int64
	Evictions   int64
	Expirations int64
}

// Metrics is a snapshot of every counter. Labels are deliberately absent: the
// documented rule is that keys, tenants and tokens never become metric labels.
type Metrics struct {
	LocalHits              int64
	LocalMisses            int64
	RemoteHits             int64
	RemoteMisses           int64
	RemoteErrors           int64
	Loads                  int64
	LoadErrors             int64
	LoadWaiters            int64
	InvalidationsPublished int64
	InvalidationsReceived  int64
	InvalidationErrors     int64
}

type counters struct {
	localHits              atomic.Int64
	localMisses            atomic.Int64
	remoteHits             atomic.Int64
	remoteMisses           atomic.Int64
	remoteErrors           atomic.Int64
	loads                  atomic.Int64
	loadErrors             atomic.Int64
	loadWaiters            atomic.Int64
	invalidationsPublished atomic.Int64
	invalidationsReceived  atomic.Int64
	invalidationErrors     atomic.Int64
}

func (c *counters) snapshot() Metrics {
	return Metrics{
		LocalHits:              c.localHits.Load(),
		LocalMisses:            c.localMisses.Load(),
		RemoteHits:             c.remoteHits.Load(),
		RemoteMisses:           c.remoteMisses.Load(),
		RemoteErrors:           c.remoteErrors.Load(),
		Loads:                  c.loads.Load(),
		LoadErrors:             c.loadErrors.Load(),
		LoadWaiters:            c.loadWaiters.Load(),
		InvalidationsPublished: c.invalidationsPublished.Load(),
		InvalidationsReceived:  c.invalidationsReceived.Load(),
		InvalidationErrors:     c.invalidationErrors.Load(),
	}
}
