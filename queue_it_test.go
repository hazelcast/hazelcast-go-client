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
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestQueueOfferTake(t *testing.T) {
	it.QueueTester(t, func(t *testing.T, q *hz.Queue) {
		targetValue := "item1"
		if ok, err := q.Add(targetValue); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		if value, err := q.Take(); err != nil {
			assert.Equal(t, targetValue, value)
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
