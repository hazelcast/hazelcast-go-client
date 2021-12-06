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
	previousCallArg int
	*testing.T
}

func (oc *orderChecker) call(arg int) {
	assert.Equal(oc.T, oc.previousCallArg+1, arg, "order of the tasks is not preserved")
	oc.previousCallArg = arg
}

func Test_defaultExecuteFnc(t *testing.T) {
	oc := &orderChecker{T: t}
	tasks := make(chan func(), 3)
	for i := 1; i < 4; i++ {
		tmp := i
		tasks <- func() {
			oc.call(tmp)
		}
	}
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go defaultExecuteFnc(tasks, quit, &wg)
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
	}, time.Second, time.Millisecond*200, "execute function did not notify about its finish")
}

func Test_serialExecutor_dispatch(t *testing.T) {
	tests := []struct {
		queueCount    uint32
		key           uint32
		expectedIndex int32
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
	for ind, tt := range tests {
		t.Run(fmt.Sprintf("QueueCount: %d, Key: %d", tt.queueCount, tt.key), func(t *testing.T) {
			se := newStripeExecutor(tt.queueCount, 0)
			tmpHandler := func() {
				panic(ind)
			}
			go se.dispatch(tt.key, tmpHandler)
			select {
			case <-se.tasks[tt.expectedIndex]:
			case <-time.After(time.Second):
				assert.FailNow(t, "dispatcher did not dispatch to correct queue")
			}
		})
	}
}

func Test_serialExecutor_start(t *testing.T) {
	t.Logf("enabled leak check")
	defer goleak.VerifyNone(t)
	t.Run("Functionality test", func(t *testing.T) {
		var orderCheckers []*orderChecker
		type pair struct {
			key     uint32
			handler func()
		}
		// create orderCheckers, index corresponding to key
		for i := 1; i <= 100; i++ {
			orderCheckers = append(orderCheckers, &orderChecker{T: t})
		}
		// populate task queues
		// assume we have orderCheckers a,b,c, we will have
		// a1,b1,c1,b2,c2,a2,a3,c3,b3
		var tasks []pair
		for i := 1; i <= 3; i++ {
			tmp := i
			for _, perm := range rand.Perm(100) {
				key := perm
				tasks = append(tasks, pair{key: uint32(key), handler: func() {
					orderCheckers[key].call(tmp)
				}})
			}
		}

		se := newStripeExecutor(3, 3)
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
	})
}
