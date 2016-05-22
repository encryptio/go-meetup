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
	// twice concurrently per concurrent key requested.
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
	MaxSize int64

	// ItemSize is called to figure out the size of a value to compare against
	// MaxSize. If ItemSize is not set or returns a value less than or equal to
	// zero, the size of a value is 1.
	ItemSize func(key string, value interface{}) int64
}

// Cache implements a meetup cache.
type Cache struct {
	o Options

	concLimitCh chan struct{}

	// mu protects m, but NOT its entries; each entry has its own lock.
	// mu also protects evictAt and totalSize.
	// Any goroutine calling mu.Lock MUST NOT hold any entry.mu locks.
	mu        sync.Mutex
	m         *tree
	evictAt   string
	totalSize int64

	t tomb.Tomb
}

type entry struct {
	// mu protects all fields in the entry
	mu        sync.Mutex
	readyCond *sync.Cond

	Size int64

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

// You must hold e.mu when calling SetReady
func (e *entry) SetReady(r bool) {
	if e.Ready != r {
		e.readyCond.Broadcast()
	}
	e.Ready = r
}

// New returns a Cache with the given Options.
func New(o Options) *Cache {
	c := &Cache{
		o: o,
		m: treeNew(),
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
	e, ok := c.m.Get(key)
	if ok {
		c.mu.Unlock()
		e.mu.Lock()
		e.RecentlyUsed = true
	} else {
		// No entry for this key. Create the entry.
		e = &entry{}
		e.readyCond = sync.NewCond(&e.mu)
		e.mu.Lock()
		c.startFill(key, e)
		c.m.Set(key, e)
		c.mu.Unlock()
	}

	age := t.Sub(e.LastUpdate)
	if c.o.ExpireAge > 0 && age >= c.o.ExpireAge {
		// This entry has expired. Clear its value and make sure it's filling.
		e.SetReady(false)
		e.Value = nil
		e.Error = nil
		if !e.Filling {
			c.startFill(key, e)
		}
	}
	if e.Ready {
		if e.Error != nil && (c.o.ErrorAge <= 0 || age >= c.o.ErrorAge) {
			e.SetReady(false)
			e.Value = nil
			e.Error = nil
			if !e.Filling {
				c.startFill(key, e)
			}
		} else if c.o.RevalidateAge > 0 && age >= c.o.RevalidateAge && !e.Filling {
			c.startFill(key, e)
		}
	}

	// Used for the test suite.
	if postGetCheckCh != nil {
		postGetCheckCh <- struct{}{}
	}

	for !e.Ready {
		e.readyCond.Wait()
	}

	value := e.Value
	err := e.Error

	e.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Cache) startFill(key string, e *entry) {
	e.Filling = true
	c.t.Go(func() error {
		c.fill(key, e)
		return nil
	})
}

func (c *Cache) fill(key string, e *entry) {
	if fillComplete != nil {
		defer func() { fillComplete <- struct{}{} }()
	}

	if c.concLimitCh != nil {
		c.concLimitCh <- struct{}{}
		defer func() { <-c.concLimitCh }()
	}

	t := now()
	value, err := c.o.Get(key)

	if c.o.MaxSize > 0 {
		e.mu.Lock()
		oldSize := e.Size
		newSize := int64(1)
		if c.o.ItemSize != nil {
			sz := c.o.ItemSize(key, value)
			if sz > 0 {
				newSize = sz
			}
		}
		e.Size = newSize
		e.mu.Unlock()

		c.mu.Lock()

		if newSize > c.o.MaxSize {
			// Rather than evict our entire cache and STILL not have room for
			// this value, we just evict this value immediately, unless it has
			// changed to a different entry in the meantime.
			v, _ := c.m.Get(key)
			if v == e {
				c.m.Delete(key)
			}
		} else {
			c.totalSize += newSize - oldSize

			if c.totalSize > c.o.MaxSize {

				// TODO: rather than evict items regardless, we can look for
				// expired values first and evict them since they'll never be
				// used.

				enum, _ := c.m.Seek(c.evictAt)
				for c.totalSize > c.o.MaxSize {
					k, v, err := enum.Next()
					if err == io.EOF {
						enum, err = c.m.SeekFirst()
						if err == io.EOF {
							// Tree is empty. Shouldn't ever occur, but we can
							// safely just bail out of the eviction loop.
							break
						}
						continue
					}

					c.evictAt = k

					v.mu.Lock()
					if v.RecentlyUsed {
						v.RecentlyUsed = false
						v.mu.Unlock()
						continue
					}

					if v.Ready {
						c.m.Delete(k)
						c.totalSize -= v.Size
					}
					v.mu.Unlock()
				}
			}
		}

		c.mu.Unlock()
	}

	e.mu.Lock()
	e.LastUpdate = t
	e.Value = value
	e.Error = err
	e.Filling = false
	e.SetReady(true)
	e.mu.Unlock()
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

	size := int64(0)
	enum, err := c.m.SeekFirst()
	if err != io.EOF {
		for {
			_, v, err := enum.Next()
			if err == io.EOF {
				break
			}
			v.mu.Lock()
			if !v.Ready {
				v.mu.Unlock()
				panic("validateTotalSize called when not all cache entries were ready")
			}
			size += v.Size
			v.mu.Unlock()
		}
	}

	if size != c.totalSize {
		panic(fmt.Sprintf("c.totalSize = %v, but calculated sum = %v", c.totalSize, size))
	}
}
