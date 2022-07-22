/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	}, time.Second, 200*time.Millisecond, "execute function did not complete")
	close(quit)
	go func() {
		wg.Wait()
		// Just to see if goroutine finished
		oc.previousCallArg = 10
	}()
	assert.Eventually(t, func() bool {
		return oc.previousCallArg == 10
	}, time.Second, 200*time.Millisecond, "execute function did not complete")
}

func TestStripeExecutor_dispatch(t *testing.T) {
	tcs := []struct {
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
	for i, tt := range tcs {
		t.Run(fmt.Sprintf("QueueCount: %d, Key: %d", tt.queueCount, tt.key), func(t *testing.T) {
			se := newStripeExecutorWithConfig(tt.queueCount, 10_000)
			task := func() {
				panic(i)
			}
			if ok := se.dispatch(tt.key, task); !ok {
				t.Fatal("could not dispatch handler")
			}
			select {
			case <-se.taskQueues[tt.expectedIndex]:
			case <-time.After(1 * time.Second):
				assert.FailNow(t, "dispatcher did not dispatch to correct queue")
			}
		})
	}
}

func TestStripeExecutor_dispatchZeroAndNegative(t *testing.T) {
	se := newStripeExecutorWithConfig(3, 10_000)
	se.start()
	defer se.stop()
	for i := 0; i <= 3; i++ {
		var job sync.WaitGroup
		job.Add(1)
		task := func() {
			job.Done()
		}
		// dispatch negative keys, assert job is done
		if ok := se.dispatch(-i, task); !ok {
			t.Fatal("could not dispatch handler")
		}
		// if job is not completed, test will fail with timeout
		job.Wait()
	}
}

func TestStripeExecutor_dispatchQueueFull(t *testing.T) {
	se := newStripeExecutorWithConfig(1, 1)
	// executor not running, make the queue full
	ok := se.dispatch(1, func() {})
	assert.True(t, ok)
	// expect unsuccessful dispatch
	ok = se.dispatch(1, func() {})
	assert.False(t, ok)
}

func TestStripeExecutor_start(t *testing.T) {
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
	var jobs sync.WaitGroup
	jobs.Add(300)
	var tasks []pair
	for i := 1; i <= 3; i++ {
		tmp := i
		for _, perm := range rand.Perm(100) {
			key := perm
			tasks = append(tasks, pair{key: key, handler: func() {
				orderCheckers[key].call(tmp)
				jobs.Done()
			}})
		}
	}

	se := newStripeExecutor()
	se.start()
	defer se.stop()
	go func() {
		for _, task := range tasks {
			if ok := se.dispatch(task.key, task.handler); !ok {
				panic("could not dispatch event handler")
			}
		}
	}()
	jobs.Wait()
	for _, oc := range orderCheckers {
		assert.Equal(t, 3, oc.previousCallArg, "task did not complete")
	}
}
