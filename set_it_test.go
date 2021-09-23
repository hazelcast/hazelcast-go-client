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
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestSet_Add(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		ok, err := s.Add(context.Background(), "value")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.Contains(context.Background(), "value"))
		assert.Equal(t, true, ok)
	})
}

func TestSet_AddAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		targetValues := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		ok, err := s.AddAll(context.Background(), targetValues...)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(context.Background(), targetValues...))
		assert.Equal(t, true, ok)
	})
}

func TestSet_AddListener(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		subscriptionID, err := s.AddItemListener(context.Background(), false, func(event *hazelcast.SetItemNotified) {
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(s.Add(context.Background(), value))
		}
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
		atomic.StoreInt32(&callCount, 0)
		if err = s.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(s.Add(context.Background(), "value2"))
		it.Never(t, func() bool { return atomic.LoadInt32(&callCount) != 0 })
	})
}

func TestSet_AddListener_IncludeValue(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const targetCallCount = int32(0)
		callCount := int32(0)
		subscriptionID, err := s.AddItemListener(context.Background(), true, func(event *hazelcast.SetItemNotified) {
			fmt.Println("value:", event.Value)
			atomic.AddInt32(&callCount, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(s.Add(context.Background(), value))
		}

		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
		atomic.StoreInt32(&callCount, 0)
		if err = s.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(s.Add(context.Background(), "value2"))
		it.Never(t, func() bool { return atomic.LoadInt32(&callCount) != 0 })
	})
}

func TestSet_Clear(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const value = "value"
		it.MustBool(s.Add(context.Background(), value))
		ok := it.MustBool(s.Contains(context.Background(), value))
		assert.Equal(t, true, ok)
		if err := s.Clear(context.Background()); err != nil {
			t.Fatal(err)
		}
		ok = it.MustBool(s.Contains(context.Background(), value))
		assert.Equal(t, false, ok)
	})
}

func TestSet_GetAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		it.MustValue(s.AddAll(context.Background(), "v1", "v2", "v3"))
		values, err := s.GetAll(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.ElementsMatch(t, []interface{}{"v1", "v2", "v3"}, values)
	})
}

func TestSet_IsEmpty(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		ok, err := s.IsEmpty(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		it.MustBool(s.Add(context.Background(), "value"))
		ok, err = s.IsEmpty(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, false, ok)
	})
}

func TestSet_Remove(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		const value = "value"
		ok := it.MustBool(s.Add(context.Background(), value))
		assert.Equal(t, true, ok)
		ok, err := s.Remove(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.IsEmpty(context.Background()))
		assert.Equal(t, true, ok)
	})
}

func TestSet_RemoveAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		values := []interface{}{int64(1), int64(2), int64(3), int64(4)}
		ok := it.MustBool(s.AddAll(context.Background(), values...))
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(context.Background(), values...))
		assert.Equal(t, true, ok)
		ok, err := s.RemoveAll(context.Background(), values...)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, ok)
		ok = it.MustBool(s.ContainsAll(context.Background(), values...))
		assert.Equal(t, false, ok)
	})
}

func TestSet_RetainAll(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		it.MustValue(s.AddAll(context.Background(), "v1", "v2", "v3"))
		ok, err := s.RetainAll(context.Background(), "v1", "v3")
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, ok)
		values := it.MustValue(s.GetAll(context.Background()))
		assert.ElementsMatch(t, []interface{}{"v1", "v3"}, values)
	})
}

func TestSet_Size(t *testing.T) {
	it.SetTester(t, func(t *testing.T, s *hazelcast.Set) {
		size, err := s.Size(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 0, size)
		it.MustValue(s.AddAll(context.Background(), "v1", "v2", "v3"))
		size, err = s.Size(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 3, size)
	})
}
