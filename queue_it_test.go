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

package hazelcast_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestQueue_Add(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		if ok, err := q.Add("value"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		assert.Equal(t, "value", it.MustValue(q.Take()))
		// TODO: set the size of the queue
		if ok, err := q.AddWithTimeout("other-value", 1*time.Second); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
	})
}

func TestQueue_AddAll(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		targetValues := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		if ok, err := q.AddAll(targetValues...); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, true, ok)
		}
		for _, value := range targetValues {
			assert.Equal(t, value, it.MustValue(q.Take()))
		}
	})
}

func TestQueue_AddListener(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		subscriptionID, err := q.AddListener(func(event *hz.QueueItemNotified) {
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(q.Add(value))
		}
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err = q.RemoveListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(q.Add("value2"))
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestQueue_AddListenerIncludeValue(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		const targetCallCount = int32(0)
		callCount := int32(0)
		subscriptionID, err := q.AddListenerIncludeValue(func(event *hz.QueueItemNotified) {
			fmt.Println("value:", event.Value)
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(q.Add(value))
		}

		time.Sleep(1 * time.Second)
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err = q.RemoveListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(q.Add("value2"))
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestQueue_Clear(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll("v1", "v2"))
		assert.Equal(t, false, it.MustValue(q.IsEmpty()))
		if err := q.Clear(); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, it.MustValue(q.IsEmpty()))
	})
}

func TestQueue_Contains(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.Add("v1"))
		if ok, err := q.Contains("v1"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, true, ok)
		}
	})
}

func TestQueue_ContainsAll(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll("v1", "v2"))
		if ok, err := q.ContainsAll("v1", "v2"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, true, ok)
		}
	})
}

func TestQueue_Drain(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		targetValues := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		it.MustValue(q.AddAll(targetValues...))
		if values, err := q.Drain(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, targetValues, values)
		}
	})
}
func TestQueue_Iterator(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		targetValues := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		it.MustValue(q.AddAll(targetValues...))
		if values, err := q.Iterator(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, targetValues, values)
		}
	})
}

func TestQueue_DrainWithMaxSize(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll(int64(1), int64(2), int64(3), int64(4)))
		if values, err := q.DrainWithMaxSize(2); err != nil {
			t.Fatal(err)
		} else {
			targetValues := []interface{}{int64(1), int64(2)}
			assert.Equal(t, targetValues, values)
		}
	})
}

func TestQueue_Peek(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		if value, err := q.Peek(); err != nil {
			t.Fatal(err)
		} else {
			assert.Nil(t, value)
		}
		it.MustValue(q.AddAll("value1", "value2"))
		if value, err := q.Peek(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "value1", value)
		}
	})
}

func TestQueue_Poll(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.Add("value1"))
		if value, err := q.Poll(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "value1", value)
		}
		if value, err := q.PollWithTimeout(1 * time.Second); err != nil {
			t.Fatal(err)
		} else {
			assert.Nil(t, value)
		}
	})
}

func TestQueue_Put(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		if err := q.Put("value"); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "value", it.MustValue(q.Take()))
	})
}

func TestQueue_RemainingCapacity(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		if remCap, err := q.RemainingCapacity(); err != nil {
			t.Fatal(err)
		} else {
			assert.Greater(t, remCap, 0)
		}
	})
}

func TestQueue_Remove(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.Add("value"))
		assert.Equal(t, false, it.MustValue(q.IsEmpty()))
		if ok, err := q.Remove("value"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		assert.Equal(t, true, it.MustValue(q.IsEmpty()))
	})
}

func TestQueue_RemoveAll(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll("v1", "v2", "v3"))
		if ok, err := q.RemoveAll("v1", "v3"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		values := it.MustValue(q.Drain())
		assert.Equal(t, []interface{}{"v2"}, values)
	})
}
func TestQueue_RetainAll(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll("v1", "v2", "v3"))
		if ok, err := q.RetainAll("v1", "v3"); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		values := it.MustValue(q.Drain())
		assert.Equal(t, []interface{}{"v1", "v3"}, values)
	})
}

func TestQueue_Size(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.AddAll("v1", "v2", "v3"))
		if size, err := q.Size(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 3, size)
		}
	})
}

func TestQueue_Take(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		it.MustValue(q.Add("value"))
		if value, err := q.Take(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "value", value)
		}
	})
}
