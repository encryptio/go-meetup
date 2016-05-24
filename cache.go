// Package meetup implements a cache with meetup and smart revalidation.
//
// A meetup cache will coalesce concurrent requests for the same item, so that
// only one request will be done to the backend per entry.
//
// In addition, this package implements a rich expiration state machine which
// helps to avoid latency spikes and provides backpressure when the backend
// returns errors.
//
// For example, if you have a dynamic proxy which proxies many hostnames to
// different places, you'll want an Options configuration similar to this:
//     op := Options{
//         Get: ...,
//
//         // Don't overload the backend with more than 50 requests at a time
//         Concurrency: 50,
//
//         // Keep errors for 2 seconds so that a failed backend doesn't get
//         // slammed with huge request load, and so that missing mappings don't
//         // cause each user request to cause a backend request.
//         //
//         // Additionally, keeping ErrorAge relatively low means that we can
//         // recover from backend failures somewhat quickly.
//         ErrorAge: 2 * time.Second,
//
//         // Keep entries for a long time so that a failed backend doesn't
//         // cause production traffic to quickly fail. We handle updating
//         // mappings with revalidation instead.
//         ExpireAge: time.Hour,
//
//         // Revalidate entries regularly so that we know their mapping points
//         // to the right place.
//         RevalidateAge: time.Minute,
//     }
package meetup

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

var (
	// ErrClosed is returned by Cache.Get when the Cache has been Closed.
	ErrClosed = errors.New("Cache Closed")
)

var (
	// now is a variable so that expiration and revalidation tests can be written
	// deterministically.
	now = time.Now

	// postGetCheckCh, if non-nil, has one value written to it per (*Cache).Get
	// that finishes all its checks and starts any fill calls.
	postGetCheckCh chan struct{}

	// fillComplete, if non-nil, has one value written to it per (*Cache).fill
	// that completes.
	fillComplete chan struct{}
)

// Options control the behavior of the Cache with respect to its backend.
type Options struct {
	// When a key is requested that does not exist in the cache (or needs to be
	// revalidated) then Get will be called. Get is called concurrently, at most
	// once concurrently per concurrent key requested.
	Get func(key string) (interface{}, error)

	// If greater than zero, only Concurrency Get calls will be done
	// concurrently. Any other calls will wait until one of the running Get calls
	// complete.
	Concurrency int

	// If an Options.Get returns an error, cache the error for this amount of
	// time. If negative or zero, don't cache errors.
	ErrorAge time.Duration

	// Once an entry's age reaches ExpireAge, it is considered expired and the
	// cached result will not be used. If set to zero, values do not expire.
	ExpireAge time.Duration

	// Once an entry's age reaches RevalidateAge, a background Options.Get will
	// be made on its key, but Cache.Get will continue to return immediately. If
	// the background Get returns an error, it will not be retried until
	// ErrorAge has passed since the last revalidation attempt.
	//
	// If zero, revalidation is disabled.
	RevalidateAge time.Duration

	// If MaxSize is greater than zero, then when Options.Get returns, some of
	// the values not used most recently will be evicted from the cache until
	// the total size of all values is underneath MaxSize.
	//
	// Currently running Gets do not count towards MaxSize.
	MaxSize uint64

	// ItemSize is called to figure out the size of a value to compare against
	// MaxSize. If ItemSize is not set or returns a zero, the size of a value is
	// assumed to be 1.
	ItemSize func(key string, value interface{}) uint64
}

// Stats are returned from Cache.Stats.
type Stats struct {
	// The number of times Cache.Get found an item in the cache.
	Hits uint64

	// The number of times Cache.Get did not find an item in the cache.
	Misses uint64

	// The number of values evicted from the cache.
	Evictions uint64

	// The number of times a value was revalidated in-place.
	Revalidations uint64

	// The number of times a value was found in the cache, but had expired.
	// NB: This counter is not updated when a value expires, only when it is
	// found in the cache after it has already expired.
	Expires uint64

	// The number of errors (err != nil) returned from Options.Get.
	Errors uint64

	// The current size of the cache.
	CurrentSize uint64

	// The current number of items in the cache.
	CurrentCount uint64
}

// Cache implements a meetup cache.
type Cache struct {
	o Options

	concLimitCh chan struct{}

	// mu protects tree, and its entries, evictAt, and totalSize.
	mu        sync.Mutex
	tree      *tree
	evictAt   string
	totalSize uint64

	stats Stats

	t tomb.Tomb
}

type entry struct {
	ReadyCond *sync.Cond

	Size uint64

	LastUpdate time.Time

	// Value and Error are valid iff Ready is true
	Value interface{}
	Error error

	// Filling is true iff a fill is running for this entry
	Filling bool

	// Only set this through SetReady.
	Ready bool

	// RecentlyUsed is true iff a Get has hit this key since the last eviction
	// cycle hit it (or since it was created.)
	RecentlyUsed bool
}

// New returns a Cache with the given Options.
func New(o Options) *Cache {
	c := &Cache{
		o:    o,
		tree: treeNew(),
	}

	if o.Concurrency > 0 {
		c.concLimitCh = make(chan struct{}, o.Concurrency)
	}

	// Keep the tomb alive for future c.fill calls
	c.t.Go(func() error {
		<-c.t.Dying()
		return nil
	})

	return c
}

func (c *Cache) setEntryValue(key string, e *entry, value interface{}, err error) {
	e.Value = value
	e.Error = err

	newSize := uint64(1)
	if c.o.ItemSize != nil {
		sz := c.o.ItemSize(key, value)
		if sz > 0 {
			newSize = sz
		}
	}

	c.totalSize += newSize - e.Size

	e.Size = newSize

	if !e.Ready {
		e.Ready = true
		e.ReadyCond.Broadcast()
	}
}

func (c *Cache) setEntryCleared(e *entry) {
	if e.Ready {
		e.Value = nil
		e.Error = nil
		c.totalSize -= e.Size
		e.Size = 0
		e.Ready = false
		e.ReadyCond.Broadcast()
	}
}

// Get retrieves an entry's value from the cache, calling Options.Get if needed
// to fill the cache. If multiple concurrent Get calls occur on the same key,
// all of them will recieve the return value of a single Options.Get call.
func (c *Cache) Get(key string) (interface{}, error) {
	select {
	case <-c.t.Dying():
		return nil, ErrClosed
	default:
	}

	t := now()

	c.mu.Lock()
	e, ok := c.tree.Get(key)
	if ok {
		e.RecentlyUsed = true
		c.stats.Hits++
	} else {
		// No entry for this key. Create the entry.
		e = &entry{
			ReadyCond: sync.NewCond(&c.mu),
		}
		c.startFill(key, e)
		c.tree.Set(key, e)
		c.stats.Misses++
	}

	if e.Ready {
		age := t.Sub(e.LastUpdate)
		if c.o.ExpireAge > 0 && age >= c.o.ExpireAge {
			c.stats.Expires++
			c.setEntryCleared(e)
			c.startFill(key, e)
		} else if e.Error != nil && (c.o.ErrorAge <= 0 || age >= c.o.ErrorAge) {
			c.setEntryCleared(e)
			c.startFill(key, e)
		} else if c.o.RevalidateAge > 0 && age >= c.o.RevalidateAge {
			c.stats.Revalidations++
			c.startFill(key, e)
		}
	}

	// Used for the test suite.
	if postGetCheckCh != nil {
		postGetCheckCh <- struct{}{}
	}

	for !e.Ready {
		e.ReadyCond.Wait()
	}

	value := e.Value
	err := e.Error

	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return value, nil
}

// Stats retrieves the current stats for the Cache.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	st := c.stats
	c.mu.Unlock()
	return st
}

func (c *Cache) startFill(key string, e *entry) {
	if e.Filling {
		return
	}

	e.Filling = true
	c.t.Go(func() error {
		c.fill(key, e)
		return nil
	})
}

func (c *Cache) fill(key string, e *entry) {

	// Used for the test suite only.
	if fillComplete != nil {
		defer func() { fillComplete <- struct{}{} }()
	}

	if c.concLimitCh != nil {
		c.concLimitCh <- struct{}{}
		defer func() { <-c.concLimitCh }()
	}

	t := now()
	value, err := c.o.Get(key)

	c.mu.Lock()

	if err != nil {
		c.stats.Errors++
	}

	e.LastUpdate = t
	e.Filling = false
	c.setEntryValue(key, e, value, err)

	if c.o.MaxSize > 0 {
		if e.Size > c.o.MaxSize {
			// Rather than evict our entire cache and STILL not have room for
			// this value, we just evict this value immediately.
			c.tree.Delete(key)
			c.totalSize -= e.Size
		} else {
			if c.totalSize > c.o.MaxSize {

				// TODO: rather than evict items regardless, we can look for
				// expired values first and evict them since they'll never be
				// used.

				enum, _ := c.tree.Seek(c.evictAt)
				for c.totalSize > c.o.MaxSize {
					k, v, err := enum.Next()
					if err == io.EOF {
						enum, err = c.tree.SeekFirst()
						if err == io.EOF {
							// Tree is empty. Shouldn't ever occur, but we can
							// safely just bail out of the eviction loop.
							break
						}
						continue
					}

					if v == e {
						// Never attempt to evict ourselves
						continue
					}

					if v.RecentlyUsed {
						v.RecentlyUsed = false
						continue
					}

					if v.Ready {
						if v.Filling {
							// We shouldn't evict keys that are filling by
							// deleting them from the map; instead, we should
							// keep them around but remove their data. This
							// allows future Cache.Gets to meet up with the
							// existing fill routine.
							c.setEntryCleared(v)
						} else {
							c.tree.Delete(k)
							c.totalSize -= v.Size
						}
						c.stats.Evictions++
					}
				}

				c.evictAt, _, _ = enum.Next()
			}
		}
	}

	c.mu.Unlock()
}

// Close waits for all running Options.Get calls to finish and makes all future
// Cache.Get calls return ErrClosed.
func (c *Cache) Close() error {
	c.t.Kill(nil)
	return c.t.Wait()
}

func (c *Cache) validateTotalSize() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.o.MaxSize <= 0 {
		panic("validateTotalSize called when Options.MaxSize is not set (no sizes are being captured)")
	}

	size := uint64(0)
	enum, err := c.tree.SeekFirst()
	if err != io.EOF {
		for {
			_, v, err := enum.Next()
			if err == io.EOF {
				break
			}
			size += v.Size
		}
	}

	if size != c.totalSize {
		panic(fmt.Sprintf("c.totalSize = %v, but calculated sum = %v", c.totalSize, size))
	}
}
