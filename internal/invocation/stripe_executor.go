package invocation

import (
	"math/rand"
	"sync"
)

// stripeExecutor executes given "tasks" preserving the order among the ones
// that are given with the same key
type stripeExecutor struct {
	quit            chan struct{}
	wg              *sync.WaitGroup
	executeFunction func(queue chan func(), quit chan struct{}, wg *sync.WaitGroup)
	tasks           []chan func()
	queueCount      uint32
}

// newStripeExecutor returns a new stripeExecutor with configured queueCount and queueSize
func newStripeExecutor(queueCount, queueSize uint32) stripeExecutor {
	se := stripeExecutor{
		tasks:      make([]chan func(), queueCount),
		queueCount: queueCount,
	}
	for ind := range se.tasks {
		se.tasks[ind] = make(chan func(), queueSize)
	}
	se.quit = make(chan struct{})
	se.wg = &sync.WaitGroup{}
	se.executeFunction = defaultExecuteFnc
	return se
}

// start fires up the workers for each queue
func (se stripeExecutor) start() {
	se.wg.Add(int(se.queueCount))
	for ind := range se.tasks {
		ind := ind
		go se.executeFunction(se.tasks[ind], se.quit, se.wg)
	}
}

// dispatch sends the handler "task" to the appropriate queue, "tasks"
// with the same key end up on the same queue
func (se stripeExecutor) dispatch(key uint32, handler func()) {
	se.tasks[key%se.queueCount] <- handler
}

func (se stripeExecutor) dispatchRandom(handler func()) {
	key := rand.Int31n(int32(se.queueCount))
	se.dispatch(uint32(key), handler)
}

// stop blocks until all workers are stopped.
func (se stripeExecutor) stop() {
	close(se.quit)
	se.wg.Wait()
}

func (se stripeExecutor) setExecutorFnc(custom func(queue chan func(), quit chan struct{}, wg *sync.WaitGroup)) {
	se.executeFunction = custom
}

func defaultExecuteFnc(queue chan func(), quit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case task := <-queue:
			task()
		case <-quit:
			return
		}
	}
}
