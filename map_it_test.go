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
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestMap_Put(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.Put(nil, "key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get(nil, "key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestMap_PutWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithTTL(nil, "key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(2 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutWithMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithMaxIdle(nil, "key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		time.Sleep(4 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutWithTTLAndMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithTTLAndMaxIdle(nil, "key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutIfAbsent(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutIfAbsent(nil, "key", targetValue); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
		if _, err := m.PutIfAbsent(nil, "key", "another-value"); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutIfAbsentWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutIfAbsentWithTTL(nil, "key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutIfAbsentWithTTLAndMaxIdle(t *testing.T) {
	t.SkipNow()
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		// TODO: better test
		if _, err := m.PutIfAbsentWithTTLAndMaxIdle(nil, "key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutTransient(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransient(nil, "key", targetValue); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutTransientWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransientWithTTL(nil, "key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutTransientWithMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransientWithMaxIdle(nil, "key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_PutTransientWithTTLAndMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		// TODO: better test
		if err := m.PutTransientWithTTLAndMaxIdle(nil, "key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get(nil, "key")))
		time.Sleep(4 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get(nil, "key")))
	})
}

func TestMap_Set(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.Set(nil, "key", targetValue); err != nil {
			t.Fatal(err)
		}
		value := it.MustValue(m.Get(nil, "key"))
		if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestMap_Delete(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set(nil, "key", targetValue))
		if value := it.MustValue(m.Get(nil, "key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Delete(nil, "key"); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get(nil, "key")); nil != value {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMap_Evict(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.Set(nil, "key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Evict(nil, "key"); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target true != %v", value)
		}
	})
}

func TestMap_Clear(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set(nil, "key", targetValue))
		if ok := it.MustBool(m.ContainsKey(nil, "key")); !ok {
			t.Fatalf("key not found")
		}
		if ok := it.MustBool(m.ContainsValue(nil, "value")); !ok {
			t.Fatalf("value not found")
		}
		value := it.MustValue(m.Get(nil, "key"))
		if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(nil); err != nil {
			t.Fatal(err)
		}
		value = it.MustValue(m.Get(nil, "key"))
		if nil != value {
			t.Fatalf("target nil!= %v", value)
		}
	})
}

func TestMap_Remove(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set(nil, "key", targetValue))
		if !it.MustBool(m.ContainsKey(nil, "key")) {
			t.Fatalf("key not found")
		}
		if value, err := m.Remove(nil, "key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target nil != %v", value)
		}
		if it.MustBool(m.ContainsKey(nil, "key")) {
			t.Fatalf("key found")
		}
	})
}

func TestMap_GetAll(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		const maxKeys = 100
		makeKey := func(id int) string {
			return fmt.Sprintf("k%d", id)
		}
		makeValue := func(id int) string {
			return fmt.Sprintf("v%d", id)
		}
		var allPairs []types.Entry
		for i := 0; i < maxKeys; i++ {
			allPairs = append(allPairs, types.NewEntry(makeKey(i), makeValue(i)))
		}
		var keys []interface{}
		var target []types.Entry
		for i, pair := range allPairs {
			if i%2 == 0 {
				keys = append(keys, pair.Key)
				target = append(target, pair)
			}
		}
		for _, pair := range allPairs {
			it.Must(m.Set(nil, pair.Key, pair.Value))
		}
		time.Sleep(1 * time.Second)
		for _, pair := range allPairs {
			it.AssertEquals(t, pair.Value, it.MustValue(m.Get(nil, pair.Key)))
		}
		if kvs, err := m.GetAll(nil, keys...); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, kvs) {
			t.Fatalf("target: %#v != %#v", target, kvs)
		}
	})
}

func TestMap_GetKeySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		it.Must(m.Set(nil, "k1", "v1"))
		it.Must(m.Set(nil, "k2", "v2"))
		it.Must(m.Set(nil, "k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get(nil, "k3")))
		if keys, err := m.GetKeySet(nil); err != nil {
			t.Fatal(err)
		} else if !assert.ElementsMatch(t, targetKeySet, keys) {
			t.FailNow()
		}
	})
}
func TestMap_GetKeySetWithPredicate(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetKeySet := []interface{}{serialization.JSON(`{"a": 15}`)}
		it.Must(m.Set(nil, serialization.JSON(`{"a": 5}`), "v1"))
		it.Must(m.Set(nil, serialization.JSON(`{"a": 10}`), "v2"))
		it.Must(m.Set(nil, serialization.JSON(`{"a": 15}`), "v3"))
		if keys, err := m.GetKeySetWithPredicate(nil, predicate.GreaterOrEqual("__key.a", 11)); err != nil {
			t.Fatal(err)
		} else if !assert.ElementsMatch(t, targetKeySet, keys) {
			t.FailNow()
		}
	})
}

func TestMap_GetValues(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValues := []interface{}{"v1", "v2", "v3"}
		it.Must(m.Set(nil, "k1", "v1"))
		it.Must(m.Set(nil, "k2", "v2"))
		it.Must(m.Set(nil, "k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get(nil, "k3")))
		if values, err := m.GetValues(nil); err != nil {
			t.Fatal(err)
		} else if !assert.ElementsMatch(t, targetValues, values) {
			t.FailNow()
		}
	})
}

func TestMap_GetValuesWithPredicate(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValues := []interface{}{serialization.JSON(`{"A": 10, "B": 200}`), serialization.JSON(`{"A": 10, "B": 30}`)}
		it.Must(m.Set(nil, "k1", serialization.JSON(`{"A": 10, "B": 200}`)))
		it.Must(m.Set(nil, "k2", serialization.JSON(`{"A": 10, "B": 30}`)))
		it.Must(m.Set(nil, "k3", serialization.JSON(`{"A": 5, "B": 200}`)))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, serialization.JSON(`{"A": 10, "B": 200}`), it.MustValue(m.Get(nil, "k1")))
		it.AssertEquals(t, serialization.JSON(`{"A": 10, "B": 30}`), it.MustValue(m.Get(nil, "k2")))
		it.AssertEquals(t, serialization.JSON(`{"A": 5, "B": 200}`), it.MustValue(m.Get(nil, "k3")))
		if values, err := m.GetValuesWithPredicate(nil, predicate.Equal("A", 10)); err != nil {
			t.Fatal(err)
		} else if !assert.ElementsMatch(t, targetValues, values) {
			t.FailNow()
		}
	})
}

func TestMap_PutAll(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		pairs := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(nil, pairs...); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get(nil, "k3")))
	})
}

func TestMap_GetEntrySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		target := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(nil, target...); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if entries, err := m.GetEntrySet(nil); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestMap_GetEntrySetWithPredicateUsingPortable(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.SerializationConfig.AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		okValue := "foo-Ğİ"
		noValue := "foo"
		entries := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: okValue, B: 10}),
			types.NewEntry("k2", &it.SamplePortable{A: noValue, B: 15}),
			types.NewEntry("k3", &it.SamplePortable{A: okValue, B: 10}),
		}
		if err := m.PutAll(nil, entries...); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: okValue, B: 10}),
			types.NewEntry("k3", &it.SamplePortable{A: okValue, B: 10}),
		}
		if entries, err := m.GetEntrySetWithPredicate(nil, predicate.And(predicate.Equal("A", okValue), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestMap_GetEntrySetWithPredicateUsingJSON(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		entries := []types.Entry{
			types.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.Json()),
			types.NewEntry("k2", it.SamplePortable{A: "foo", B: 15}.Json()),
			types.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.Json()),
		}
		if err := m.PutAll(nil, entries...); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []types.Entry{
			types.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.Json()),
			types.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.Json()),
		}
		if entries, err := m.GetEntrySetWithPredicate(nil, predicate.And(predicate.Equal("A", "foo"), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestMap_GetEntryView(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", "v1"))
		if ev, err := m.GetEntryView(nil, "k1"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, "k1", ev.Key())
			it.AssertEquals(t, "v1", ev.Value())
		}
	})
}

func TestMap_AddIndexWithConfig(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", serialization.JSON(`{"A": 10, "B": 40}`)))
		indexConfig := types.IndexConfig{
			Name:               "my-index",
			Type:               types.IndexTypeBitmap,
			Attributes:         []string{"A"},
			BitmapIndexOptions: types.BitmapIndexOptions{UniqueKey: "B", UniqueKeyTransformation: types.UniqueKeyTransformationLong},
		}
		if err := m.AddIndexWithConfig(nil, indexConfig); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMap_AddIndexValidationError(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		indexConfig := types.IndexConfig{
			Name:               "my-index",
			Type:               types.IndexTypeBitmap,
			Attributes:         []string{"A", "B"},
			BitmapIndexOptions: types.BitmapIndexOptions{UniqueKey: "B", UniqueKeyTransformation: types.UniqueKeyTransformationLong},
		}
		if err := m.AddIndexWithConfig(nil, indexConfig); err == nil {
			t.Fatalf("should have failed")
		} else {
			vErr := &hz.IndexValidationError{}
			if !errors.As(err, &vErr) {
				t.Fatalf("should have returned an index validation error")
			}
		}
	})
}

func TestMap_Flush(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", "v1"))
		if err := m.Flush(nil); err != nil {
			t.Fatal(err)
		}
	})
}

// TODO: Test Map AddInterceptor
// TODO: Test Map ExecuteOnEntries
// TODO: Test Map LockWithLease
// TODO: Test Map SetTTL
// TODO: Test Map SetWithTTL
// TODO: Test Map SetWithTTLAndMaxIdle
// TODO: Test Map TryLock
// TODO: Test Map TryLockWithLease
// TODO: Test Map TryLockWithTimeout
// TODO: Test Map TryLockWithLeaseTimeout
// TODO: Test Map TryPut
// TODO: Test Map TryPutWithTimeout
// TODO: Test Map TryRemove
// TODO: Test Map TryRemoveWithTimeout

func TestMap_LoadAllWithoutReplacing(t *testing.T) {
	makeMapName := func() string {
		return "test-map"
	}
	it.MapTesterWithConfigAndName(t, makeMapName, nil, func(t *testing.T, m *hz.Map) {
		putSampleKeyValues(m, 2)
		it.Must(m.EvictAll(nil))
		it.Must(m.PutTransient(nil, "k0", "new-v0"))
		it.Must(m.PutTransient(nil, "k1", "new-v1"))
		time.Sleep(1 * time.Second)
		it.Must(m.LoadAllWithoutReplacing(nil, "k0", "k1"))
		targetEntrySet := []types.Entry{
			{Key: "k0", Value: "new-v0"},
			{Key: "k1", Value: "new-v1"},
		}
		entrySet := it.MustValue(m.GetAll(nil, "k0", "k1")).([]types.Entry)
		if !entriesEqualUnordered(targetEntrySet, entrySet) {
			t.Fatalf("target %#v != %#v", targetEntrySet, entrySet)
		}
	})
}

func TestMap_LoadAllReplacing(t *testing.T) {
	makeMapName := func() string {
		return "test-map"
	}
	it.MapTesterWithConfigAndName(t, makeMapName, nil, func(t *testing.T, m *hz.Map) {
		keys := putSampleKeyValues(m, 10)
		it.Must(m.EvictAll(nil))
		it.Must(m.LoadAllReplacing(nil))
		entrySet := it.MustValue(m.GetAll(nil, keys...)).([]types.Entry)
		if len(keys) != len(entrySet) {
			t.Fatalf("target len: %d != %d", len(keys), len(entrySet))
		}
		it.Must(m.EvictAll(nil))
		keys = keys[:5]
		it.Must(m.LoadAllReplacing(nil, keys...))
		entrySet = it.MustValue(m.GetAll(nil, keys...)).([]types.Entry)
		if len(keys) != len(entrySet) {
			t.Fatalf("target len: %d != %d", len(keys), len(entrySet))
		}
	})
}

func TestContextMap_Lock(t *testing.T) {
	it.MapTester(t, func(t *testing.T, cm *hz.Map) {
		const goroutineCount = 100
		const key = "counter"
		wg := &sync.WaitGroup{}
		wg.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func() {
				defer wg.Done()
				intValue := int64(0)
				lockCtx := cm.NewLockContext(nil)
				if err := cm.Lock(lockCtx, key); err != nil {
					panic(err)
				}
				defer cm.Unlock(lockCtx, key)
				v, err := cm.Get(lockCtx, key)
				if err != nil {
					panic(err)
				}
				if v != nil {
					intValue = v.(int64)
				}
				intValue++
				if err = cm.Set(lockCtx, key, intValue); err != nil {
					panic(err)
				}
			}()
		}
		wg.Wait()
		if lastValue, err := cm.Get(context.Background(), key); err != nil {
			panic(err)
		} else {
			assert.Equal(t, int64(goroutineCount), lastValue)
		}
	})
}

func TestContextMap_ForceUnlock(t *testing.T) {
	it.MapTester(t, func(t *testing.T, cm *hz.Map) {
		lockCtx := cm.NewLockContext(nil)
		if err := cm.Lock(lockCtx, "k1"); err != nil {
			t.Fatal(err)
		}
		if locked, err := cm.IsLocked(lockCtx, "k1"); err != nil {
			log.Fatal(err)
		} else {
			it.AssertEquals(t, true, locked)
		}
		if err := cm.ForceUnlock(lockCtx, "k1"); err != nil {
			log.Fatal(err)
		}
		if locked, err := cm.IsLocked(lockCtx, "k1"); err != nil {
			log.Fatal(err)
		} else {
			assert.Equal(t, false, locked)
		}
	})
}

func TestMap_IsEmptySize(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if value, err := m.IsEmpty(nil); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target: true != false")
		}
		targetSize := 0
		if value, err := m.Size(nil); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		it.MustValue(m.Put(nil, "k1", "v1"))
		it.MustValue(m.Put(nil, "k2", "v2"))
		it.MustValue(m.Put(nil, "k3", "v3"))
		if value, err := m.IsEmpty(nil); err != nil {
			t.Fatal(err)
		} else if value {
			t.Fatalf("target: false != true")
		}
		targetSize = 3
		if value, err := m.Size(nil); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
	})
}

func TestMap_RemoveAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.SerializationConfig.AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		entries := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: "foo", B: 10}),
			types.NewEntry("k2", &it.SamplePortable{A: "foo", B: 15}),
			types.NewEntry("k3", &it.SamplePortable{A: "foo", B: 10}),
		}
		it.Must(m.PutAll(nil, entries...))
		time.Sleep(1 * time.Second)
		if err := m.RemoveAll(nil, predicate.Equal("B", 10)); err != nil {
			t.Fatal(err)
		}
		target := []types.Entry{
			types.NewEntry("k2", &it.SamplePortable{A: "foo", B: 15}),
		}
		if kvs, err := m.GetAll(nil, "k1", "k2", "k3"); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, kvs) {
			t.Fatalf("target: %#v != %#v", target, kvs)
		}

	})
}

func TestMap_RemoveIfSame(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", "v1"))
		it.Must(m.Set(nil, "k2", "v2"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k2")))
		if removed, err := m.RemoveIfSame(nil, "k1", "v1"); err != nil {
			t.Fatal(err)
		} else if !removed {
			t.Fatalf("not removed")
		}
		it.AssertEquals(t, false, it.MustValue(m.ContainsKey(nil, "k1")))
		if removed, err := m.RemoveIfSame(nil, "k2", "v1"); err != nil {
			t.Fatal(err)
		} else if removed {
			t.Fatalf("removed")
		}
		it.AssertEquals(t, true, it.MustValue(m.ContainsKey(nil, "k2")))
	})
}

func TestMap_Replace(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", "v1"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		if oldValue, err := m.Replace(nil, "k1", "v2"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, "v1", oldValue)
		}
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k1")))
	})
}

func TestMap_ReplaceIfSame(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(nil, "k1", "v1"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get(nil, "k1")))
		if replaced, err := m.ReplaceIfSame(nil, "k1", "v1", "v2"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, true, replaced)
		}
		it.AssertEquals(t, "v2", it.MustValue(m.Get(nil, "k1")))
	})
}

func TestMap_EntryNotifiedEvent(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		const totalCallCount = int32(100)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
		}
		listenerConfig.NotifyEntryAdded(true)
		subscriptionID, err := m.AddEntryListener(nil, listenerConfig, handler)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(totalCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(nil, key, value))
		}
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, totalCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err := m.RemoveEntryListener(nil, subscriptionID); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(totalCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(nil, key, value))
		}
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestMap_EntryNotifiedEventToKey(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Key:          "k1",
		}
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(nil, listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put(nil, "k1", "v1"))
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, int32(1), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestMap_EntryNotifiedEventWithPredicate(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.SerializationConfig.AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		const totalCallCount = int32(100)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Predicate:    predicate.Equal("A", "foo"),
		}
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(nil, listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(totalCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := &it.SamplePortable{A: "foo", B: int32(i)}
			it.MustValue(m.Put(nil, key, value))
		}
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, totalCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestMap_EntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.SerializationConfig.AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Key:          "k1",
			Predicate:    predicate.Equal("A", "foo"),
		}
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(nil, listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put(nil, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(nil, "k1", &it.SamplePortable{A: "bar", B: 10}))
		it.MustValue(m.Put(nil, "k2", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, int32(1), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
	})
}

func TestMap_Destroy(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if err := m.Destroy(nil); err != nil {
			t.Fatal(err)
		}
	})
}

// ==== Reliability Tests ====

func TestMapSetGet1000(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		const setGetCount = 1000
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			it.Must(m.Set(nil, key, value))
		}
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			targetValue := fmt.Sprintf("v%d", i)
			if !assert.Equal(t, targetValue, it.MustValue(m.Get(nil, key))) {
				t.FailNow()
			}
		}
	})
}

func TestMapSetGetLargePayload(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		const payloadSize = 1 * 1024 * 1024
		payload := make([]byte, payloadSize)
		for i := 0; i < len(payload); i++ {
			payload[i] = byte(i)
		}
		it.Must(m.Set(nil, "k1", payload))
		v := it.MustValue(m.Get(nil, "k1"))
		if !assert.Equal(t, payload, v) {
			t.FailNow()
		}
	})
}

func makeStringSet(items []interface{}) map[string]struct{} {
	result := map[string]struct{}{}
	for _, item := range items {
		result[item.(string)] = struct{}{}
	}
	return result
}

func entriesEqualUnordered(p1, p2 []types.Entry) bool {
	if len(p1) != len(p2) {
		return false
	}
	// inefficient
	for _, item1 := range p1 {
		if entriesIndex(item1, p2) < 0 {
			return false
		}
	}
	for _, item2 := range p2 {
		if entriesIndex(item2, p1) < 0 {
			return false
		}
	}
	return true
}

func entriesIndex(p types.Entry, ps []types.Entry) int {
	for i, item := range ps {
		if reflect.DeepEqual(p, item) {
			return i
		}
	}
	return -1
}

func putSampleKeyValues(m *hz.Map, count int) []interface{} {
	keys := []interface{}{}
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		it.MustValue(m.Put(nil, key, value))
		keys = append(keys, key)
	}
	return keys
}
