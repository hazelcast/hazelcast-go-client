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
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestSet_Add(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		ok, err := s.Add("value")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.Contains("value"))
		assert.Equal(t, true, ok)
	})
}

func TestSet_AddAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		targetValues := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		ok, err := s.AddAll(targetValues...)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(targetValues...))
		assert.Equal(t, true, ok)
	})
}

func TestSet_AddListener(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		subscriptionID, err := s.AddListener(func(event *hazelcast.SetItemNotified) {
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(s.Add(value))
		}
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err = s.RemoveListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(s.Add("value2"))
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestSet_AddListenerIncludeValue(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const targetCallCount = int32(0)
		callCount := int32(0)
		subscriptionID, err := s.AddListenerIncludeValue(func(event *hazelcast.SetItemNotified) {
			fmt.Println("value:", event.Value)
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(s.Add(value))
		}

		time.Sleep(1 * time.Second)
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err = s.RemoveListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(s.Add("value2"))
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestSet_Clear(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const value = "value"
		it.MustBool(s.Add(value))
		ok := it.MustBool(s.Contains(value))
		assert.Equal(t, true, ok)
		if err := s.Clear(); err != nil {
			t.Fatal(err)
		}
		ok = it.MustBool(s.Contains(value))
		assert.Equal(t, false, ok)
	})
}

func TestSet_GetAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		it.MustValue(s.AddAll("v1", "v2", "v3"))
		values, err := s.GetAll()
		if err != nil {
			log.Fatal(err)
		}
		assert.ElementsMatch(t, []interface{}{"v1", "v2", "v3"}, values)
	})
}

func TestSet_IsEmpty(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		ok, err := s.IsEmpty()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		it.MustBool(s.Add("value"))
		ok, err = s.IsEmpty()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, false, ok)
	})
}

func TestSet_Remove(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const value = "value"
		ok := it.MustBool(s.Add(value))
		assert.Equal(t, true, ok)
		ok, err := s.Remove(value)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.IsEmpty())
		assert.Equal(t, true, ok)
	})
}

func TestSet_RemoveAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		values := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		ok := it.MustBool(s.AddAll(values...))
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(values...))
		assert.Equal(t, true, ok)
		ok, err := s.RemoveAll(values...)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(values...))
		assert.Equal(t, false, ok)
	})
}

func TestSet_RetainAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		it.MustValue(s.AddAll("v1", "v2", "v3"))
		ok, err := s.RetainAll("v1", "v3")
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, ok)
		values := it.MustValue(s.GetAll())
		assert.ElementsMatch(t, []interface{}{"v1", "v3"}, values)
	})
}

func TestSet_Size(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		size, err := s.Size()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 0, size)
		it.MustValue(s.AddAll("v1", "v2", "v3"))
		size, err = s.Size()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 3, size)
	})
}
