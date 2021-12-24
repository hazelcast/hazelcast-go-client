package invocation

import (
	"math/rand"
	"runtime"
	"sync"
)

var (
	// Default values differ from java impl. Also queue size is calculated differently.
	// Java Client: queueSize per worker = defaultEventQueueCapacity / defaultEventWorkerCount
	// Go Client: queueSize per worker = defaultEventQueueCapacity
	defaultEventQueueCapacity = 10000
	defaultEventWorkerCount   = uint32(runtime.NumCPU())
)

// executor represents the function that will run on workers of stripeExecutor.
type executor func(queue chan func(), quit chan struct{}, wg *sync.WaitGroup)

// stripeExecutor executes given "tasks" preserving the order among the ones that are given with the same key.
type stripeExecutor struct {
	quit       chan struct{}
	wg         *sync.WaitGroup
	execFn     executor
	taskQueues []chan func()
	queueCount uint32
}

// newStripeExecutor returns a new stripeExecutor with default configuration.
func newStripeExecutor() stripeExecutor {
	return newStripeExecutorWithConf(defaultEventWorkerCount, defaultEventWorkerCount)
}

// newStripeExecutor returns a new stripeExecutor with configured queueCount and queueSize.
func newStripeExecutorWithConf(queueCount, queueSize uint32) stripeExecutor {
	se := stripeExecutor{
		taskQueues: make([]chan func(), queueCount),
		queueCount: queueCount,
	}
	for ind := range se.taskQueues {
		se.taskQueues[ind] = make(chan func(), queueSize)
	}
	se.quit = make(chan struct{})
	se.wg = &sync.WaitGroup{}
	se.execFn = defaultExecFn
	return se
}

// start fires up the workers for each queue.
func (se stripeExecutor) start() {
	se.wg.Add(int(se.queueCount))
	for ind := range se.taskQueues {
		ind := ind
		go se.execFn(se.taskQueues[ind], se.quit, se.wg)
	}
}

// dispatch sends the handler "task" to one of the appropriate taskQueues, "tasks" with the same key end up on the same queue.
func (se stripeExecutor) dispatch(key uint32, task func()) {
	se.taskQueues[key%se.queueCount] <- task
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

func (se stripeExecutor) setExecutorFnc(f executor) {
	se.execFn = f
}

func defaultExecFn(queue chan func(), quit chan struct{}, wg *sync.WaitGroup) {
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
