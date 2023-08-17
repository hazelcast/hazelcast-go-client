/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"reflect"
	"sync/atomic"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestReplicatedMap_Put(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		if _, err := m.Put(context.Background(), "key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get(context.Background(), "key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestReplicatedMap_Clear(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		it.MustValue(m.Put(context.Background(), "key", targetValue))
		if ok := it.MustBool(m.ContainsKey(context.Background(), "key")); !ok {
			t.Fatalf("key not found")
		}
		it.Eventually(t, func() bool { return it.MustBool(m.ContainsValue(context.Background(), "value")) })
		if value := it.MustValue(m.Get(context.Background(), "key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(context.Background()); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool { return it.MustValue(m.Get(context.Background(), "key")) == nil })
	})
}

func TestReplicatedMap_Remove(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		it.MustValue(m.Put(context.Background(), "key", targetValue))
		if !it.MustBool(m.ContainsKey(context.Background(), "key")) {
			t.Fatalf("key not found")
		}
		if value, err := m.Remove(context.Background(), "key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target nil != %v", value)
		}
		if it.MustBool(m.ContainsKey(context.Background(), "key")) {
			t.Fatalf("key found")
		}
	})
}

func TestReplicatedMap_GetEntrySet(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		target := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(context.Background(), target...); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool {
			entries, err := m.GetEntrySet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return entriesEqualUnordered(target, entries)
		})
	})
}

func TestReplicatedMap_GetKeySet(t *testing.T) {
	it.ReplicatedMapTester(t, func(t *testing.T, m *hz.ReplicatedMap) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		it.MustValue(m.Put(context.Background(), "k1", "v1"))
		it.MustValue(m.Put(context.Background(), "k2", "v2"))
		it.MustValue(m.Put(context.Background(), "k3", "v3"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get(context.Background(), "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(context.Background(), "k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get(context.Background(), "k3")))
		it.Eventually(t, func() bool {
			keys, err := m.GetKeySet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return reflect.DeepEqual(makeStringSet(targetKeySet), makeStringSet(keys))
		})
	})
}
func TestReplicatedMap_GetValues(t *testing.T) {
	it.ReplicatedMapTester(t, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValues := []interface{}{"v1", "v2", "v3"}
		it.MustValue(m.Put(context.Background(), "k1", "v1"))
		it.MustValue(m.Put(context.Background(), "k2", "v2"))
		it.MustValue(m.Put(context.Background(), "k3", "v3"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get(context.Background(), "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(context.Background(), "k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get(context.Background(), "k3")))
		it.Eventually(t, func() bool {
			values, err := m.GetValues(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return reflect.DeepEqual(makeStringSet(targetValues), makeStringSet(values))
		})
	})
}

func TestReplicatedMap_IsEmptySize(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		if value, err := m.IsEmpty(context.Background()); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target: true != false")
		}
		targetSize := 0
		if value, err := m.Size(context.Background()); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		it.MustValue(m.Put(context.Background(), "k1", "v1"))
		it.MustValue(m.Put(context.Background(), "k2", "v2"))
		it.MustValue(m.Put(context.Background(), "k3", "v3"))
		it.Eventually(t, func() bool {
			value, err := m.IsEmpty(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return !value
		})
		targetSize = 3
		it.Eventually(t, func() bool {
			value, err := m.Size(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return targetSize == value
		})
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEvent(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		subscriptionID, err := m.AddEntryListener(context.Background(), handler)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(context.Background(), key, value))
		}
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
		atomic.StoreInt32(&callCount, 0)
		if err := m.RemoveEntryListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put(context.Background(), "k1", "v1"); err != nil {
			t.Fatal(err)
		}
		it.Never(t, func() bool { return atomic.LoadInt32(&callCount) != 0 })
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEventWithKey(t *testing.T) {
	it.ReplicatedMapTesterWithConfig(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		if _, err := m.AddEntryListenerToKey(context.Background(), "k1", handler); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			value := fmt.Sprintf("value-%d", i)
			if _, err := m.Put(context.Background(), "k1", value); err != nil {
				t.Fatal(err)
			}
		}
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
	})
}

func TestReplicatedMap_AddEntryListenerWithPredicate(t *testing.T) {
	it.ReplicatedMapTester(t, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = int32(1)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		if _, err := m.AddEntryListenerWithPredicate(context.Background(), predicate.SQL("this == foo"), handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put(context.Background(), "k1", "foo"))
		it.MustValue(m.Put(context.Background(), "k1", "foo2"))
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
	})
}

func TestReplicatedMap_AddEntryListenerToKeyWithPredicate(t *testing.T) {
	it.ReplicatedMapTester(t, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = int32(1)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		if _, err := m.AddEntryListenerToKeyWithPredicate(context.Background(), "k1", predicate.SQL("this == foo"), handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put(context.Background(), "k1", "foo"))
		it.MustValue(m.Put(context.Background(), "k2", "foo"))
		it.MustValue(m.Put(context.Background(), "k1", "foo2"))
		it.Eventually(t, func() bool {
			cc := atomic.LoadInt32(&callCount)
			t.Logf("call count: %d", cc)
			return cc == targetCallCount
		})
	})
}
