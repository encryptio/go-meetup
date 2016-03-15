package meetup

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

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

func mustGet(t *testing.T, c *Cache, key string, want interface{}) {
	v, err := c.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, v, want)
	}
}

func TestCache(t *testing.T) {
	hits := 0
	c := NewCache(Options{
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
	var mu sync.Mutex
	hits := 0

	c := NewCache(Options{
		Get: func(key string) (interface{}, error) {
			mu.Lock()
			hits++
			mu.Unlock()
			return key, nil
		},
		ExpireAge: time.Second,
	})
	defer c.Close()

	mustGet(t, c, "a", "a")
	mu.Lock()
	if hits != 1 {
		t.Fatalf("hits = %v after first use", hits)
	}
	mu.Unlock()

	advanceTime(2 * time.Second)

	mustGet(t, c, "a", "a")
	mu.Lock()
	if hits != 2 {
		t.Fatalf("hits != 2 after second use")
	}
	mu.Unlock()

	advanceTime(time.Second / 2)

	mustGet(t, c, "a", "a")
	mu.Lock()
	if hits != 2 {
		t.Fatalf("hits = %v after third use", hits)
	}
	mu.Unlock()
}

func TestExpiryUsesStartTime(t *testing.T) {
	hits := 0

	c := NewCache(Options{
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

	c := NewCache(Options{
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

	c := NewCache(Options{
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
	c := NewCache(Options{
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
	fillComplete = make(chan struct{})
	defer func() { fillComplete = nil }()

	blockGets := newBoolWatcher(false)
	var getReadyToBlock chan struct{}

	var mu sync.Mutex
	hits := 0
	finished := 0

	c := NewCache(Options{
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

func TestErrorCaching(t *testing.T) {
	deliberate := errors.New("deliberate failure")

	var mu sync.Mutex
	hits := 0

	c := NewCache(Options{
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
