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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestMultiMap_Put(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetValue := "value1"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "key", targetValue)), "multi-map put failed")
		tmp := it.MustValue(m.Get(context.Background(), "key"))
		it.AssertEquals(t, []interface{}{targetValue}, tmp)

		targetValue2 := "value2"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "key", targetValue2)), "multi-map put failed")
		tmp = it.MustValue(m.Get(context.Background(), "key"))
		assert.ElementsMatch(t, []interface{}{targetValue, targetValue2}, tmp)
	})
}

//todo check if this putAll api addresses the need
func TestMultiMap_PutAll(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		existingValue := "v4"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "k1", existingValue)), "multimap put failed")
		value := it.MustValue(m.Get(context.Background(), "k1"))
		it.AssertEquals(t, []interface{}{existingValue}, value)
		it.Must(m.PutAll(context.Background(), "k1", "v1", "v2", "v3"))
		assert.ElementsMatch(t, []string{"v1", "v2", "v3", "v4"}, it.MustSlice(m.Get(context.Background(), "k1")))
	})
}

func TestMultiMap_Delete(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetValue := "value"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "key", targetValue)), "multimap put failed")
		value := it.MustValue(m.Get(context.Background(), "key"))
		it.AssertEquals(t, []interface{}{targetValue}, value)
		it.Must(m.Delete(context.Background(), "key"))
		if value := it.MustSlice(m.Get(context.Background(), "key")); len(value) != 0 {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMultiMap_Clear(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetValue := "value"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "key", targetValue)), "multi-map put failed")
		value := it.MustValue(m.Get(context.Background(), "key"))
		it.AssertEquals(t, []interface{}{targetValue}, value) // check value
		it.Must(m.Clear(context.Background()))
		if value := it.MustSlice(m.Get(context.Background(), "key")); len(value) != 0 {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMultiMap_Remove(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetValue := "value"
		assert.False(t, !it.MustBool(m.Put(context.Background(), "key", targetValue)), "multi-map put failed")
		value := it.MustValue(m.Get(context.Background(), "key"))
		it.AssertEquals(t, []interface{}{targetValue}, value) // check value
		it.Must(m.Clear(context.Background()))
		if value := it.MustSlice(m.Get(context.Background(), "key")); len(value) != 0 {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMultiMap_GetKeySet(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		targets := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		for _, target := range targets {
			assert.False(t, !it.MustBool(m.Put(context.Background(), target.Key, target.Value)), "multi-map put failed")
		}
		assert.ElementsMatch(t, targetKeySet, it.MustSlice(m.GetKeySet(context.Background())))
	})
}

func TestMultiMap_GetValues(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetValues := []interface{}{"v1", "v2", "v3"}
		targets := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		for _, target := range targets {
			assert.False(t, !it.MustBool(m.Put(context.Background(), target.Key, target.Value)), "multi-map put failed")
		}
		for _, target := range targets {
			it.AssertEquals(t, []interface{}{target.Value}, it.MustValue(m.Get(context.Background(), target.Key)))
		}
		assert.ElementsMatch(t, targetValues, it.MustSlice(m.GetValues(context.Background())))
	})
}

func TestMultiMap_GetEntrySet(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targets := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		for _, target := range targets {
			assert.False(t, !it.MustBool(m.Put(context.Background(), target.Key, target.Value)), "multi-map put failed")
		}
		if entries, err := m.GetEntrySet(context.Background()); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(targets, entries) {
			t.Fatalf("target: %#v != %#v", targets, entries)
		}
	})
}

func TestMultiMap_Lock(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, cm *hz.MultiMap) {
		const goroutineCount = 100
		const key = "counter"
		wg := &sync.WaitGroup{}
		wg.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func() {
				defer wg.Done()
				intValue := int64(0)
				lockCtx := cm.NewLockContext(context.Background())
				if err := cm.Lock(lockCtx, key); err != nil {
					panic(err)
				}
				defer cm.Unlock(lockCtx, key)
				v, err := cm.Get(lockCtx, key)
				if err != nil {
					panic(err)
				}
				if len(v) != 0 {
					intValue = v[0].(int64)
				}
				intValue++
				if _, err := cm.Remove(lockCtx, key); err != nil {
					panic(err)
				}
				if successful, err := cm.Put(lockCtx, key, intValue); err != nil {
					panic(err)
				} else if !successful {
					panic("operation multi-map put failed")
				}
			}()
		}
		wg.Wait()
		it.AssertEquals(t, []interface{}{int64(goroutineCount)}, it.MustValue(cm.Get(context.Background(), key)))
	})
}

func TestMultiMap_ForceUnlockLock(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, cm *hz.MultiMap) {
		lockCtx := cm.NewLockContext(context.Background())
		if err := cm.Lock(lockCtx, "k1"); err != nil {
			t.Fatal(err)
		}
		if locked, err := cm.IsLocked(lockCtx, "k1"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, true, locked)
		}
		if err := cm.ForceUnlock(lockCtx, "k1"); err != nil {
			t.Fatal(err)
		}
		if locked, err := cm.IsLocked(lockCtx, "k1"); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, false, locked)
		}
	})
}

func TestMultiMap_Size(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		targetSize := 0
		if value, err := m.Size(context.Background()); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		it.MustValue(m.Put(context.Background(), "k1", "v1"))
		it.MustValue(m.Put(context.Background(), "k2", "v2"))
		it.MustValue(m.Put(context.Background(), "k3", "v3"))
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

func TestMultiMap_EntryNotifiedEvent(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		const totalCallCount = int32(100)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			if event.EventType == hz.EntryAdded {
				atomic.AddInt32(&callCount, 1)
			}
		}
		listenerConfig := hz.MultiMapEntryListenerConfig{
			IncludeValue: true,
		}
		listenerConfig.NotifyEntryAdded(true)
		subscriptionID, err := m.AddEntryListener(context.Background(), listenerConfig, handler)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(totalCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(context.Background(), key, value))
		}
		it.Eventually(t, func() bool {
			return atomic.LoadInt32(&callCount) == totalCallCount
		})
		atomic.StoreInt32(&callCount, 0)
		if err := m.RemoveEntryListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(totalCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(context.Background(), key, value))
		}
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestMultiMap_EntryNotifiedEventToKey(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		listenerConfig := hz.MultiMapEntryListenerConfig{
			IncludeValue: true,
			Key:          "k1",
		}
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(context.Background(), listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put(context.Background(), "k1", "v1"))
		it.Eventually(t, func() bool {
			return atomic.LoadInt32(&callCount) == int32(1)
		})
	})
}

func TestMultiMap_Destroy(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		if err := m.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMultiMap_TryLock(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		const key = "foo"
		go func() {
			ctx := m.NewLockContext(context.Background())
			it.Must(m.Lock(ctx, key))
			wg.Done()
		}()
		wg.Wait()
		mainCtx := m.NewLockContext(context.Background())
		b, err := m.TryLock(mainCtx, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.False(t, b)
	})
}

func TestMultiMap_LockWithLease(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		const key = "foo"
		go func() {
			ctx := m.NewLockContext(context.Background())
			it.Must(m.LockWithLease(ctx, key, 100*time.Millisecond))
			wg.Done()
		}()
		wg.Wait()
		mainCtx := m.NewLockContext(context.Background())
		err := m.Lock(mainCtx, key)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestMultiMap_TryLockWithLease(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		const key = "foo"
		go func() {
			ctx := m.NewLockContext(context.Background())
			if b := it.MustValue(m.TryLockWithLease(ctx, key, 100*time.Millisecond)); b != true {
				panic("unexpected value")
			}
			wg.Done()
		}()
		wg.Wait()
		mainCtx := m.NewLockContext(context.Background())
		err := m.Lock(mainCtx, key)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestMultiMap_TryLockWithTimeout(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		const key = "foo"
		go func() {
			ctx := m.NewLockContext(context.Background())
			it.Must(m.Lock(ctx, key))
			wg.Done()
		}()
		wg.Wait()
		mainCtx := m.NewLockContext(context.Background())
		b, err := m.TryLockWithTimeout(mainCtx, key, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		assert.False(t, b)
	})
}

func TestMultiMap_TryLockWithLeaseAndTimeout(t *testing.T) {
	it.MultiMapTester(t, func(t *testing.T, m *hz.MultiMap) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		const key = "foo"
		go func() {
			ctx := m.NewLockContext(context.Background())
			it.Must(m.Lock(ctx, key))
			wg.Done()
		}()
		wg.Wait()
		mainCtx := m.NewLockContext(context.Background())
		b, err := m.TryLockWithLeaseAndTimeout(mainCtx, key, 100*time.Millisecond, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		assert.False(t, b)
	})
}
