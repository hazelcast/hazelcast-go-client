package invocation

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

type orderChecker struct {
	sync.Mutex
	previousCallArg int
}

func (oc *orderChecker) call(arg int) {
	oc.Lock()
	defer oc.Unlock()
	if oc.previousCallArg+1 != arg {
		panic("order of the tasks is not preserved")
	}
	oc.previousCallArg = arg
}

func Test_defaultExecFn(t *testing.T) {
	var oc orderChecker
	tasks := make(chan func(), 10_000)
	for i := 1; i < 4; i++ {
		tmp := i
		tasks <- func() {
			oc.call(tmp)
		}
	}
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go defaultExecFn(tasks, quit, &wg)
	assert.Eventually(t, func() bool {
		return oc.previousCallArg == 3
	}, time.Second, time.Millisecond*200, "execute function could not finish the tasks")
	close(quit)
	go func() {
		wg.Wait()
		// Just to see if goroutine finished
		oc.previousCallArg = 10
	}()
	assert.Eventually(t, func() bool {
		return oc.previousCallArg == 10
	}, time.Second, 200*time.Millisecond, "execute function did not notify about its finish")
}

func Test_serialExecutor_dispatch(t *testing.T) {
	tests := []struct {
		queueCount    int
		key           int
		expectedIndex int
	}{
		{
			queueCount:    4,
			key:           2,
			expectedIndex: 2,
		},
		{
			queueCount:    2,
			key:           2,
			expectedIndex: 0,
		},
		{
			queueCount:    2,
			key:           3,
			expectedIndex: 1,
		},
		{
			queueCount:    2,
			key:           4,
			expectedIndex: 0,
		},
		{
			queueCount:    1,
			key:           5,
			expectedIndex: 0,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("QueueCount: %d, Key: %d", tt.queueCount, tt.key), func(t *testing.T) {
			se, err := newStripeExecutorWithConfig(tt.queueCount, 10_000)
			assert.Nil(t, err)
			tmpHandler := func() {
				panic(i)
			}
			go se.dispatch(tt.key, tmpHandler)
			select {
			case <-se.taskQueues[tt.expectedIndex]:
			case <-time.After(time.Second):
				assert.FailNow(t, "dispatcher did not dispatch to correct queue")
			}
		})
	}
}

func Test_serialExecutor_dispatchQueueFull(t *testing.T) {
	se, err := newStripeExecutorWithConfig(1, 1)
	assert.Nil(t, err)
	// executor not running, make the queue full
	qFull := se.dispatch(1, func() {})
	assert.False(t, qFull)
	// expect unsuccessful dispatch
	qFull = se.dispatch(1, func() {})
	assert.True(t, qFull)
}

func Test_serialExecutor_start(t *testing.T) {
	t.Logf("enabled leak check")
	defer goleak.VerifyNone(t)
	var orderCheckers []*orderChecker
	type pair struct {
		handler func()
		key     int
	}
	// create orderCheckers, index corresponding to key
	for i := 1; i <= 100; i++ {
		var checker orderChecker
		orderCheckers = append(orderCheckers, &checker)
	}
	// populate task queues
	// assume we have orderCheckers a,b,c, we will have
	// a1,b1,c1,b2,c2,a2,a3,c3,b3
	var tasks []pair
	for i := 1; i <= 3; i++ {
		tmp := i
		for _, perm := range rand.Perm(100) {
			key := perm
			tasks = append(tasks, pair{key: key, handler: func() {
				orderCheckers[key].call(tmp)
			}})
		}
	}

	se := newStripeExecutor()
	se.start()
	go func() {
		for _, task := range tasks {
			se.dispatch(task.key, task.handler)
		}
	}()
	time.Sleep(time.Second * 1)
	for _, oc := range orderCheckers {
		assert.Equal(t, 3, oc.previousCallArg, "task did not complete")
	}
	se.stop()
}
