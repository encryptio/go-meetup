package meetup

import "sync"

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
