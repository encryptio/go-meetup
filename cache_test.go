package meetup

import (
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// Fake time implementation

var (
	fakeTimeMu sync.Mutex
	fakeTime   = time.Now()
)

func init() {
	now = func() time.Time {
		fakeTimeMu.Lock()
		t := fakeTime
		fakeTimeMu.Unlock()
		return t
	}
}

func advanceTime(d time.Duration) {
	fakeTimeMu.Lock()
	fakeTime = fakeTime.Add(d)
	fakeTimeMu.Unlock()
}

////////////////////////////////////////////////////////////////////////////////
// boolwatcher

type boolWatcher struct {
	mu sync.Mutex
	c  *sync.Cond
	v  bool
}

func newBoolWatcher(value bool) *boolWatcher {
	w := &boolWatcher{
		v: value,
	}
	w.c = sync.NewCond(&w.mu)
	return w
}

func (w *boolWatcher) Wait(value bool) {
	w.mu.Lock()
	for w.v != value {
		w.c.Wait()
	}
	w.mu.Unlock()
}

func (w *boolWatcher) Set(value bool) {
	w.mu.Lock()
	w.v = value
	w.c.Broadcast()
	w.mu.Unlock()
}

func (w *boolWatcher) Get() bool {
	w.mu.Lock()
	v := w.v
	w.mu.Unlock()
	return v
}

////////////////////////////////////////////////////////////////////////////////
// Utility functions

func mustGet(t *testing.T, c *Cache, key string, want interface{}) {
	v, err := c.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, v, want)
	}
}

func atomicHitCheck(t *testing.T, value *uint64, expect uint64) {
	actual := atomic.LoadUint64(value)
	if actual != expect {
		t.Fatalf("hits = %v, but wanted %v", actual, expect)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Tests

func TestCache(t *testing.T) {
	hits := 0
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			hits++
			return key, nil
		},
	})
	defer c.Close()

	if hits != 0 {
		t.Fatalf("hits != 0 after init")
	}

	mustGet(t, c, "a", "a")
	if hits != 1 {
		t.Fatalf("hits != 1 after first use")
	}

	mustGet(t, c, "b", "b")
	if hits != 2 {
		t.Fatalf("hits = %v after second use", hits)
	}

	mustGet(t, c, "a", "a")
	if hits != 2 {
		t.Fatalf("hits != 2 after third use")
	}
}

func TestExpiry(t *testing.T) {
	hits := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			hits++
			return key, nil
		},
		ExpireAge: time.Second,
	})
	defer c.Close()

	mustGet(t, c, "a", "a")
	if hits != 1 {
		t.Fatalf("hits = %v after first use", hits)
	}

	advanceTime(2 * time.Second)

	mustGet(t, c, "a", "a")
	if hits != 2 {
		t.Fatalf("hits != 2 after second use")
	}

	advanceTime(time.Second / 2)

	mustGet(t, c, "a", "a")
	if hits != 2 {
		t.Fatalf("hits = %v after third use", hits)
	}
}

func TestExpiryUsesStartTime(t *testing.T) {
	hits := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			advanceTime(time.Second)
			hits++
			return key, nil
		},
		ExpireAge: time.Second,
	})
	defer c.Close()

	mustGet(t, c, "a", "a")
	if hits != 1 {
		t.Fatalf("hits != 1 after first use")
	}

	mustGet(t, c, "a", "a")
	if hits != 2 {
		t.Fatalf("hits = %v after second use", hits)
	}
}

func TestMeetup(t *testing.T) {
	postGetCheckCh = make(chan struct{})
	defer func() { postGetCheckCh = nil }()

	const concurrency = 1000

	blockGets := newBoolWatcher(true)

	var mu sync.Mutex
	hits := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			blockGets.Wait(false)

			mu.Lock()
			c := hits
			hits++
			mu.Unlock()
			return c, nil
		},
	})
	defer c.Close()

	vals := make(chan interface{})
	for i := 0; i < concurrency; i++ {
		go func() {
			v, err := c.Get("a")
			if err != nil {
				t.Errorf(`c.Get("a") returned error %v`, err)
			}
			vals <- v
		}()
	}

	for i := 0; i < concurrency; i++ {
		<-postGetCheckCh
	}

	blockGets.Set(false)

	for i := 0; i < concurrency; i++ {
		v := <-vals
		if !reflect.DeepEqual(v, 0) {
			t.Errorf("Cache did not meet up. Wanted value %v, got %v", 0, v)
		}
	}
}

func TestConcurrencyLimit(t *testing.T) {
	blockGets := newBoolWatcher(true)

	const (
		workers = 1000
		limit   = 3
	)

	var mu sync.Mutex
	conc := 0
	maxConc := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			mu.Lock()
			conc++
			if conc > maxConc {
				maxConc = conc
			}
			mu.Unlock()

			blockGets.Wait(false)

			mu.Lock()
			conc--
			mu.Unlock()

			return key, nil
		},
		Concurrency: limit,
	})
	defer c.Close()

	vals := make(chan interface{})

	for i := 0; i < workers; i++ {
		go func(i int) {
			k := strconv.FormatInt(int64(i), 10)
			v, _ := c.Get(k)
			vals <- v
		}(i)
	}

	blockGets.Set(false)

	for i := 0; i < workers; i++ {
		<-vals
	}

	if conc != 0 {
		t.Errorf("Options.Get still running after Cache.Get returned")
	}

	t.Logf("max concurrency seen was %v", maxConc)
	if maxConc > limit {
		t.Errorf("max concurrency of %v is over limit %v", maxConc, limit)
	}
}

func TestCacheDoesntKeepErrors(t *testing.T) {
	deliberateErr := errors.New("deliberate failure")

	hits := 0
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			hits++
			return nil, deliberateErr
		},
	})
	defer c.Close()

	v, err := c.Get("a")
	if v != nil {
		t.Errorf("Got unexpected value %#v from c.Get", v)
	}
	if err != deliberateErr {
		t.Errorf("Got unexpected error %v from c.Get", err)
	}

	// NB: no time passes here

	v, err = c.Get("a")
	if v != nil {
		t.Errorf("Got unexpected value %#v from c.Get", v)
	}
	if err != deliberateErr {
		t.Errorf("Got unexpected error %v from c.Get", err)
	}

	if hits != 2 {
		t.Errorf("Hits was %v, not 2", hits)
	}
}

func TestRevalidation(t *testing.T) {
	fillComplete = make(chan struct{}, 1)
	defer func() { fillComplete = nil }()

	blockGets := newBoolWatcher(false)
	var getReadyToBlock chan struct{}

	var mu sync.Mutex
	hits := 0
	finished := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			mu.Lock()
			hits++
			myHits := hits
			mu.Unlock()
			if getReadyToBlock != nil {
				getReadyToBlock <- struct{}{}
			}
			blockGets.Wait(false)
			mu.Lock()
			finished++
			mu.Unlock()
			return myHits, nil
		},
		RevalidateAge: time.Second,
	})
	defer c.Close()

	mustGet(t, c, "a", 1)
	<-fillComplete
	if hits != 1 {
		t.Errorf("hits != 1")
	}

	blockGets.Set(true)
	advanceTime(time.Second)

	getReadyToBlock = make(chan struct{}, 1)
	mustGet(t, c, "a", 1) // NB: does not block with background revalidation
	<-getReadyToBlock
	mu.Lock()
	if hits != 2 {
		t.Errorf("hits = %v, wanted 2", hits)
	}
	if finished != 1 {
		t.Errorf("finished != 1")
	}
	mu.Unlock()

	blockGets.Set(false)
	<-fillComplete

	getReadyToBlock = nil
	mustGet(t, c, "a", 2)
}

func TestRevalidationIgnoresErrors(t *testing.T) {
	fillComplete = make(chan struct{}, 1)
	defer func() { fillComplete = nil }()

	deliberate := errors.New("deliberate failure")

	var erroring uint32
	var returnValue uint32

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			if atomic.LoadUint32(&erroring) == 1 {
				return nil, deliberate
			}
			return int(atomic.LoadUint32(&returnValue)), nil
		},
		RevalidateAge: time.Second,
	})
	defer c.Close()

	// Original get works normally
	mustGet(t, c, "a", 0)
	<-fillComplete

	advanceTime(time.Second)
	atomic.StoreUint32(&returnValue, 1)

	// Further gets are revalidated in the background
	mustGet(t, c, "a", 0)
	<-fillComplete
	mustGet(t, c, "a", 1)

	atomic.StoreUint32(&returnValue, 2)
	atomic.StoreUint32(&erroring, 1)
	advanceTime(time.Second)

	// Errors are NOT returned but cause Options.Get to run repeatedly
	mustGet(t, c, "a", 1)
	<-fillComplete
	mustGet(t, c, "a", 1)
	<-fillComplete

	atomic.StoreUint32(&erroring, 0)

	// Finally, recovering causes caching to work again
	mustGet(t, c, "a", 1)
	<-fillComplete
	mustGet(t, c, "a", 2)
	mustGet(t, c, "a", 2)
}

func TestRevalidationCachesErrors(t *testing.T) {
	fillComplete = make(chan struct{}, 1)
	defer func() { fillComplete = nil }()

	deliberate := errors.New("deliberate failure")

	var erroring uint32

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			if atomic.LoadUint32(&erroring) == 1 {
				return nil, deliberate
			}
			return 0, nil
		},
		RevalidateAge: time.Second * 2,
		ErrorAge:      time.Second,
	})
	defer c.Close()

	// Original get works normally
	mustGet(t, c, "a", 0)
	<-fillComplete

	atomic.StoreUint32(&erroring, 1)
	advanceTime(time.Second * 2)

	// Revalidation occurs
	mustGet(t, c, "a", 0)
	<-fillComplete
	// But only once per ErrorAge
	mustGet(t, c, "a", 0)
	mustGet(t, c, "a", 0)

	advanceTime(time.Second)

	// Once per ErrorAge, not once per RevalidateAge
	mustGet(t, c, "a", 0)
	<-fillComplete
	mustGet(t, c, "a", 0)
	mustGet(t, c, "a", 0)

	atomic.StoreUint32(&erroring, 0)
	advanceTime(time.Second)

	// After recovering, caches normally
	mustGet(t, c, "a", 0)
	<-fillComplete
	mustGet(t, c, "a", 0)

	advanceTime(time.Second)

	mustGet(t, c, "a", 0)
}

func TestErrorCaching(t *testing.T) {
	deliberate := errors.New("deliberate failure")

	var mu sync.Mutex
	hits := 0

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			mu.Lock()
			hits++
			mu.Unlock()
			return nil, deliberate
		},
		ErrorAge: time.Second,
	})
	defer c.Close()

	_, err := c.Get("a")
	if err != deliberate {
		t.Errorf("Got unexpected error %v from Get", err)
	}

	_, err = c.Get("a")
	if err != deliberate {
		t.Errorf("Got unexpected error %v from Get", err)
	}

	if hits != 1 {
		t.Errorf("error was not cached")
	}

	advanceTime(time.Second)

	deliberate = errors.New("other deliberate failure")
	_, err = c.Get("a")
	if err != deliberate {
		t.Errorf("Got unexpected error %v from Get", err)
	}

	if hits != 2 {
		t.Errorf("hits = %v", hits)
	}
}

func TestClosedGet(t *testing.T) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return key, nil
		},
	})
	c.Close()

	_, err := c.Get("a")
	if err != ErrClosed {
		t.Errorf("Got unexpected error %v from Get", err)
	}
}

func TestGetMeetupCreateRace(t *testing.T) {
	// This test was written to maximize the likelihood of a read lock to write
	// lock transition in Cache.Get noticing that the entry has already been
	// created even though it already transitioned to a write lock, but the
	// Cache logic has changed to use a basic Mutex for better performance.

	const (
		workers         = 5
		iterations      = 1000
		extraGetsPerKey = 2 * workers
	)

	var hits uint64
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return key, nil
		},
	})
	defer c.Close()

	var keyInt uint64 = 1

	done := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		go func() {
			for i := 0; i < iterations; i++ {
				for i := 0; i < extraGetsPerKey; i++ {
					c.Get(strconv.FormatUint(atomic.LoadUint64(&keyInt), 10))
				}

				newKeyInt := atomic.AddUint64(&keyInt, 1)
				c.Get(strconv.FormatUint(newKeyInt, 10))
			}
			done <- struct{}{}
		}()
	}

	for worker := 0; worker < workers; worker++ {
		<-done
	}

	gotHits := atomic.LoadUint64(&hits)
	gotKeys := atomic.LoadUint64(&keyInt)

	if gotHits != gotKeys {
		t.Errorf("made %v keys, but got %v hits", gotKeys, gotHits)
	}

	if gotKeys != iterations*workers+1 {
		t.Errorf("created %v keys but wanted %v", gotKeys, iterations*workers+1)
	}

	t.Logf("key at end was %v", gotKeys)
}

func TestTinyMaxSize(t *testing.T) {
	var hits uint64
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return nil, nil
		},
		MaxSize: 1,
	})
	defer c.Close()

	// We should only be able to cache a single value.

	c.validateTotalSize()
	atomicHitCheck(t, &hits, 0)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 1)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 1)

	mustGet(t, c, "b", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 2)

	mustGet(t, c, "b", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 2)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 3)

	mustGet(t, c, "b", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 4)
}

func TestEvictionKeepsHotKeys(t *testing.T) {
	var hits uint64
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return nil, nil
		},
		MaxSize: 3,
	})
	defer c.Close()

	// Pre-cache the "hot" item
	mustGet(t, c, "hot", nil)
	atomicHitCheck(t, &hits, 1)

	for loop := 0; loop < 2; loop++ {
		for i := 1; i <= 100; i++ {
			// One half of requests are scanning the keyspace
			mustGet(t, c, strconv.FormatInt(int64(i), 10), nil)
			expectedHits := uint64(i + 1 + loop*100)
			atomicHitCheck(t, &hits, expectedHits)

			// And one half is just spamming the "hot" key, which should stay
			// cached
			mustGet(t, c, "hot", nil)
			atomicHitCheck(t, &hits, expectedHits)
			c.validateTotalSize()
		}
	}
}

func TestEvictionKeepsMeetup(t *testing.T) {
	// Makes an entry evict while it is revalidating. The entry should still
	// meet up with other gets in this case.

	postGetCheckCh = make(chan struct{}, 1)
	defer func() { postGetCheckCh = nil }()

	var aHits uint64
	blockA := newBoolWatcher(false)
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			if key == "a" {
				atomic.AddUint64(&aHits, 1)
				blockA.Wait(false)
			}
			return nil, nil
		},
		RevalidateAge: time.Second,
		MaxSize:       1,
	})

	mustGet(t, c, "a", nil)
	<-postGetCheckCh
	atomicHitCheck(t, &aHits, 1)

	advanceTime(time.Second)

	blockA.Set(true)
	mustGet(t, c, "a", nil) // starts revalidation but returns
	<-postGetCheckCh
	mustGet(t, c, "b", nil) // evicts value
	<-postGetCheckCh

	done := make(chan struct{})
	go func() {
		defer close(done)
		mustGet(t, c, "a", nil)
	}()
	<-postGetCheckCh

	blockA.Set(false)
	<-done

	atomicHitCheck(t, &aHits, 2)
}

func TestItemSize(t *testing.T) {
	var hits uint64
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return nil, nil
		},
		MaxSize: 4,
		ItemSize: func(key string, value interface{}) uint64 {
			return 4
		},
	})
	defer c.Close()

	c.validateTotalSize()
	atomicHitCheck(t, &hits, 0)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 1)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 1)

	mustGet(t, c, "b", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 2)

	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 3)
}

func TestItemSizeTooBig(t *testing.T) {
	var hits uint64
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return nil, nil
		},
		MaxSize: 2,
		ItemSize: func(key string, value interface{}) uint64 {
			if key == "big" {
				return 99
			}
			return 1
		},
	})
	defer c.Close()

	// Fill the cache to its max size
	mustGet(t, c, "a", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 1)

	mustGet(t, c, "b", nil)
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 2)

	// "big" items should not be cached
	mustGet(t, c, "big", nil)
	atomicHitCheck(t, &hits, 3)
	c.validateTotalSize()

	mustGet(t, c, "big", nil)
	atomicHitCheck(t, &hits, 4)
	c.validateTotalSize()

	// Other items were NOT evicted from the cache
	mustGet(t, c, "a", nil)
	atomicHitCheck(t, &hits, 4)
	mustGet(t, c, "b", nil)
	atomicHitCheck(t, &hits, 4)
	c.validateTotalSize()
}

func TestItemSizeChanges(t *testing.T) {
	fillComplete = make(chan struct{}, 1)
	defer func() { fillComplete = nil }()

	var hits uint64
	size := uint64(1)
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			atomic.AddUint64(&hits, 1)
			return atomic.LoadUint64(&size), nil
		},
		MaxSize: 2,
		ItemSize: func(key string, value interface{}) uint64 {
			return value.(uint64)
		},
		RevalidateAge: time.Second,
	})
	defer c.Close()

	c.validateTotalSize()
	atomicHitCheck(t, &hits, 0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mustGet(t, c, "a", uint64(1))
		mustGet(t, c, "b", uint64(1))
		mustGet(t, c, "a", uint64(1))
		mustGet(t, c, "b", uint64(1))
	}()
	<-fillComplete
	<-fillComplete
	<-done
	c.validateTotalSize()
	atomicHitCheck(t, &hits, 2)

	advanceTime(time.Second)
	atomic.StoreUint64(&size, 2)

	mustGet(t, c, "a", uint64(1))
	<-fillComplete
	mustGet(t, c, "a", uint64(2))

	c.validateTotalSize()
	atomicHitCheck(t, &hits, 3)
}

func TestStats(t *testing.T) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			if key == "error" {
				return nil, errors.New("err")
			}
			return nil, nil
		},
		MaxSize: 4,
		ItemSize: func(key string, value interface{}) uint64 {
			return 2
		},
		ExpireAge:     time.Second * 3,
		RevalidateAge: time.Second,
		ErrorAge:      time.Second * 2,
	})
	defer c.Close()

	var expectStats Stats
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	c.Get("a")
	expectStats.Misses++
	expectStats.CurrentSize = 2
	expectStats.CurrentCount = 1
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	c.Get("a")
	expectStats.Hits++
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	c.Get("error")
	expectStats.Misses++
	expectStats.Errors++
	expectStats.CurrentSize = 4
	expectStats.CurrentCount = 2
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	c.Get("b")
	expectStats.Misses++
	expectStats.Evictions++
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	advanceTime(time.Second)

	fillComplete = make(chan struct{})
	go c.Get("b")
	<-fillComplete
	fillComplete = nil
	expectStats.Revalidations++
	expectStats.Hits++
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}

	advanceTime(time.Second * 4)
	c.Get("b")
	expectStats.Expires++
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}
}

func TestStatsOverMaxSize(t *testing.T) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
		MaxSize: 1,
		ItemSize: func(key string, value interface{}) uint64 {
			return 5
		},
	})
	defer c.Close()

	c.Get("a")

	expectStats := Stats{
		Misses:    1,
		Evictions: 1,
	}
	if expectStats != c.Stats() {
		t.Errorf("Wanted Stats() = %#v, but got %#v", expectStats, c.Stats())
	}
}

func TestExpiryWithoutKeyReuseStillExpires(t *testing.T) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
		ExpireAge: time.Second,
		// NB: no MaxSize limit
	})
	defer c.Close()

	for i := 0; i < 10000; i++ {
		_, err := c.Get(strconv.Itoa(i))
		if err != nil {
			t.Error(err)
			break
		}
		advanceTime(time.Second)

		c.validateTotalSize()
	}

	size := c.Stats().CurrentSize
	t.Logf("Size after 10000 Gets is %v", size)
	if size > 100 {
		t.Errorf("Wanted size <= 100, but got %v", size)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Benchmarks

func BenchmarkGetCreateSerial(b *testing.B) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
	})
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Get(strconv.FormatInt(int64(i), 10))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetCreate8Parallel(b *testing.B) {
	const workers = 8

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
	})
	defer c.Close()

	keysPerWorker := b.N / workers

	var wg sync.WaitGroup
	start := make(chan struct{})
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			<-start
			for i := keysPerWorker * r; i < keysPerWorker*(r+1); i++ {
				_, err := c.Get(strconv.FormatInt(int64(i), 10))
				if err != nil {
					b.Fatal(err)
				}
			}
		}(r)
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkGetCachedSerial(b *testing.B) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
	})
	defer c.Close()
	c.Get("")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("")
	}
}

func BenchmarkGetCached8Parallel(b *testing.B) {
	const workers = 8

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
	})
	defer c.Close()

	c.Get("")
	keysPerWorker := b.N / workers

	var wg sync.WaitGroup
	start := make(chan struct{})
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < keysPerWorker; i++ {
				_, err := c.Get("")
				if err != nil {
					b.Fatal(err)
				}
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkGetCreateLimitedSizeSerial(b *testing.B) {
	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
		MaxSize: 128,
	})
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Get(strconv.FormatInt(int64(i), 10))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetCreateLimitedSize8Parallel(b *testing.B) {
	const workers = 8

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
		MaxSize: 128,
	})
	defer c.Close()

	keysPerWorker := b.N / workers

	var wg sync.WaitGroup
	start := make(chan struct{})
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			<-start
			for i := keysPerWorker * r; i < keysPerWorker*(r+1); i++ {
				_, err := c.Get(strconv.FormatInt(int64(i), 10))
				if err != nil {
					b.Fatal(err)
				}
			}
		}(r)
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkZipfLimited(b *testing.B) {
	// A somewhat realistic probability distribution of keys.

	c := New(Options{
		Get: func(key string) (interface{}, error) {
			return nil, nil
		},
		MaxSize: 10000,
	})
	defer c.Close()

	const keyCount = 500000

	rng := rand.New(rand.NewSource(4422))
	z := rand.NewZipf(rng, 1.1, 7, keyCount-1)

	keys := make([]string, keyCount)
	for i := 0; i < len(keys); i++ {
		// Randomized key space to spread out popular values; if popular values
		// are clustered together, that uses less of the CPU cache for them and
		// overestimates performance.
		keys[i] = strconv.FormatInt(rng.Int63(), 16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := z.Uint64()
		_, err := c.Get(keys[int(val)])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
