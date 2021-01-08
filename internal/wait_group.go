package internal

import (
	"sync"
	"sync/atomic"
	"time"
)

type TimedWaitGroup struct {
	sync.WaitGroup

	// Counter.
	// Higher 32bits: decremented by both wg.Done() and timeout.
	// Lower 32bits: decremented only by wg.Done().
	counter uint64
}

func (wg *TimedWaitGroup) Add(delta int) {
	wg.WaitGroup.Add(delta)
	d := uint32(delta)
	atomic.AddUint64(&wg.counter, combineToUint64(d, d))
}

func (wg *TimedWaitGroup) Done() {
	c := atomic.LoadUint64(&wg.counter)
	hc, lc := splitUint64(c)
	for hc > 0 {
		if atomic.CompareAndSwapUint64(&wg.counter, c, combineToUint64(hc-1, lc-1)) {
			wg.WaitGroup.Done()
			return
		}
		c = atomic.LoadUint64(&wg.counter)
		hc, lc = splitUint64(c)
	}
}

// Once Await returns, further calls to Wait() will return immediately
func (wg *TimedWaitGroup) Await(d time.Duration) bool {
	time.AfterFunc(d, func() {
		c := atomic.LoadUint64(&wg.counter)
		hc, lc := splitUint64(c)
		for hc > 0 {
			if atomic.CompareAndSwapUint64(&wg.counter, c, combineToUint64(0, lc)) {
				wg.WaitGroup.Add(-int(hc))
				break
			}
			c = atomic.LoadUint64(&wg.counter)
			hc, lc = splitUint64(c)
		}
	})

	wg.Wait()
	return atomic.LoadUint64(&wg.counter) == 0
}

func splitUint64(c uint64) (uint32, uint32) {
	return uint32(c >> 32), uint32(c)
}

func combineToUint64(x, y uint32) uint64 {
	return uint64(x)<<32 | uint64(y)
}
