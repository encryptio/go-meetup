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
//
//         // Keep the cache from taking up too much memory.
//         MaxSize: 10000,
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
//
// All ages are relative to when the Options.Get call starts, not when it
// finishes.
type Options struct {
	// When a key is requested that does not exist in the cache (or needs to be
	// revalidated) then Get will be called for that key.
	//
	// If multiple concurrent Cache.Get calls mention the same key, they will be
	// coalesced to a single Options.Get call and all Cache.Get callers will
	// receive the same value and error return.
	Get func(key string) (interface{}, error)

	// If greater than zero, only Concurrency Options.Get calls will be done
	// concurrently. Any other calls will wait until one of the running Get calls
	// complete.
	Concurrency int

	// Once an entry's age reaches ExpireAge, it is considered expired and the
	// cached result will not be used.
	//
	// If set to zero, values do not expire.
	ExpireAge time.Duration

	// If an Options.Get returns an error, and ErrorAge is set, then the error
	// will be saved in the cache for ErrorAge. Once that time passes, the error
	// will expire and Options.Get will be retried.
	//
	// Note that it doesn't make sense to set ErrorAge and ExpireAge with
	// ErrorAge > ExpireAge; error values will expire based on the overall
	// expiration before ErrorAge can take effect.
	ErrorAge time.Duration

	// Once an entry's age reaches RevalidateAge, a background Options.Get will
	// be made on its key, but Cache.Get will continue to return immediately. If
	// the background Get returns an error, it will not be saved, and if
	// ErrorAge is set, will not be retried until ErrorAge has passed since the
	// last revalidation attempt.
	//
	// During this process, the existing entry will continue to be returned from
	// Cache.Get as long as it has not expired.
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
	//
	// If an item reports its size to be larger than MaxSize, then it is evicted
	// immediately instead of the other entries in the cache.
	ItemSize func(key string, value interface{}) uint64
}

// Stats are returned from Cache.Stats.
type Stats struct {
	// The number of times Cache.Get returned cached data.
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
	t tomb.Tomb

	o Options

	concLimitCh chan struct{}

	entryPool *sync.Pool

	mu            sync.Mutex
	tree          *tree
	evictAt       *enumerator
	expireCheckAt *enumerator
	totalSize     uint64
	stats         Stats
}

type entry struct {
	ReadyCond *sync.Cond

	Size uint64

	LastUpdate          time.Time
	DontRevalidateUntil time.Time

	// Value and Error are valid iff Ready is true
	Value interface{}
	Error error

	// ReadersWaiting is a counter of the number of *Cache.Gets which have a
	// pointer to this entry and intend to read its Value and Error fields.
	ReadersWaiting uint32

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

	c.entryPool = &sync.Pool{
		New: func() interface{} {
			return &entry{
				ReadyCond: sync.NewCond(&c.mu),
			}
		},
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

func (c *Cache) doneWithEntry(e *entry) {
	if e.Filling {
		panic("cannot be done with an entry that is filling")
	}

	if e.ReadersWaiting != 0 {
		// This entry will have its Value and Error fields read later; it must
		// not be cleared before then. Therefore, we just drop it on the floor
		// and let the GC handle it.
		//
		// Performance is still relatively good without entry pooling, so it's
		// not terrible to create a little garbage.
		return
	}

	// Zero all fields except ReadyCond
	*e = entry{ReadyCond: e.ReadyCond}

	c.entryPool.Put(e)
}

func (c *Cache) getNewEntry() *entry {
	return c.entryPool.Get().(*entry)
}

func (c *Cache) setEntryValue(key string, e *entry, value interface{}, err error) {
	e.Value = value
	e.Error = err
	e.DontRevalidateUntil = time.Time{}

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
// to fill the cache. Details on the semantics are documented on Options.
func (c *Cache) Get(key string) (interface{}, error) {
	select {
	case <-c.t.Dying():
		return nil, ErrClosed
	default:
	}

	t := now()

	fillInline := false

	c.mu.Lock()
	e, ok := c.tree.Get(key)
	if ok {
		e.RecentlyUsed = true
	} else {
		// No entry for this key. Create the entry.
		e = c.getNewEntry()
		fillInline = true
		c.tree.Set(key, e)
		c.stats.Misses++
	}

	e.ReadersWaiting++

	if e.Ready {
		age := t.Sub(e.LastUpdate)
		if c.o.ExpireAge > 0 && age >= c.o.ExpireAge {
			c.stats.Expires++
			c.setEntryCleared(e)
			fillInline = true
		} else if e.Error != nil && (c.o.ErrorAge <= 0 || age >= c.o.ErrorAge) {
			c.setEntryCleared(e)
			fillInline = true
		} else if c.o.RevalidateAge > 0 && age >= c.o.RevalidateAge &&
			(t.Equal(e.DontRevalidateUntil) || t.After(e.DontRevalidateUntil)) {

			c.stats.Revalidations++
			c.startFill(key, e)
		}
		if e.Ready {
			c.stats.Hits++
		}
	}

	// Used for the test suite.
	if postGetCheckCh != nil {
		postGetCheckCh <- struct{}{}
	}

	if fillInline {
		if e.Ready || e.Filling {
			panic("not reached")
		}
		e.Filling = true
		c.mu.Unlock()
		c.fill(key, e)
		c.mu.Lock()
	}

	for !e.Ready {
		e.ReadyCond.Wait()
	}

	value := e.Value
	err := e.Error

	e.ReadersWaiting--

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
	st.CurrentSize = c.totalSize
	st.CurrentCount = uint64(c.tree.Len())
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

	if !e.Ready || err == nil {
		e.LastUpdate = t
		c.setEntryValue(key, e, value, err)
	}

	if e.Ready && err != nil && c.o.ErrorAge > 0 {
		e.DontRevalidateUntil = t.Add(c.o.ErrorAge)
	}

	e.Filling = false

	if err != nil {
		c.stats.Errors++
	}

	if c.o.ExpireAge > 0 || c.o.ErrorAge > 0 {
		c.expireCheckStep(t)
	}

	if c.o.MaxSize > 0 {
		if e.Size > c.o.MaxSize {
			// Rather than evict our entire cache and STILL not have room for
			// this value, we just evict this value immediately.
			c.tree.Delete(key)
			c.totalSize -= e.Size
			c.stats.Evictions++
		} else {
			c.evictCheck(e)
		}
	}

	c.mu.Unlock()
}

func (c *Cache) expireCheckStep(t time.Time) {
	for i := 0; i < 2; i++ {
		var (
			k   string
			v   *entry
			err error = io.EOF
		)
		if c.expireCheckAt != nil {
			k, v, err = c.expireCheckAt.Next()
		}
		if err == io.EOF {
			c.expireCheckAt, err = c.tree.SeekFirst()
			if err == io.EOF {
				// Tree is empty
				return
			}
			continue
		}

		if v.Ready && !v.Filling {
			age := t.Sub(v.LastUpdate)
			expired := false
			if v.Error != nil && c.o.ErrorAge > 0 && age >= c.o.ErrorAge {
				expired = true
			} else if c.o.ExpireAge > 0 && age >= c.o.ExpireAge {
				expired = true
			}
			if expired {
				c.stats.Expires++
				c.tree.Delete(k)
				c.totalSize -= v.Size
				c.doneWithEntry(v)
			}
		}
	}
}

func (c *Cache) evictCheck(invulnerableEntry *entry) {
	var (
		k   string
		v   *entry
		err error = io.EOF
	)
	for c.totalSize > c.o.MaxSize {
		if c.evictAt != nil {
			k, v, err = c.evictAt.Next()
		}
		if err == io.EOF {
			c.evictAt, err = c.tree.SeekFirst()
			if err == io.EOF {
				// Tree is empty. Shouldn't ever occur, but we can
				// safely just bail out of the eviction loop.
				break
			}
			continue
		}

		if v == invulnerableEntry {
			// Never attempt to evict the invulnerable entry that was just
			// inserted.
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
				c.doneWithEntry(v)
			}
			c.stats.Evictions++
		}
	}
}

// Close cleans up any helper goroutines and makes all future Cache.Get calls
// return ErrClosed.
func (c *Cache) Close() error {
	c.t.Kill(nil)
	return c.t.Wait()
}

func (c *Cache) validateTotalSize() {
	c.mu.Lock()
	defer c.mu.Unlock()

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
