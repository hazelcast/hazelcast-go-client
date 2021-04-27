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

func TestMapPutGet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.Put("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestMapPutWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithTTL("key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutWithMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithMaxIdle("key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutWithTTLAndMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutWithTTLAndMaxIdle("key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		assert.Equal(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutIfAbsent(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutIfAbsent("key", targetValue); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		if _, err := m.PutIfAbsent("key", "another-value"); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
	})
}

func TestMapPutIfAbsentWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if _, err := m.PutIfAbsentWithTTL("key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutIfAbsentWithTTLAndMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		// TODO: better test
		if _, err := m.PutIfAbsentWithTTLAndMaxIdle("key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutTransient(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransient("key", targetValue); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
	})
}

func TestMapPutTransientWithTTL(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransientWithTTL("key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutTransientWithMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.PutTransientWithMaxIdle("key", targetValue, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestMapPutTransientWithTTLAndMaxIdle(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		// TODO: better test
		if err := m.PutTransientWithTTLAndMaxIdle("key", targetValue, 1*time.Second, 1*time.Second); err != nil {
			t.Fatal(err)
		}
		it.AssertEquals(t, targetValue, it.MustValue(m.Get("key")))
		time.Sleep(2 * time.Second)
		it.AssertEquals(t, nil, it.MustValue(m.Get("key")))
	})
}

func TestSetGetMap(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.Set("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestSetGet1000(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		const setGetCount = 1000
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			it.Must(m.Set(key, value))
		}
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			targetValue := fmt.Sprintf("v%d", i)
			it.AssertEquals(t, targetValue, it.MustValue(m.Get(key)))
		}
	})
}

func TestDelete(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
		if value := it.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Delete("key"); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMapEvict(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		if err := m.Set("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Evict("key"); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target true != %v", value)
		}
	})
}

func TestMapClearSetGet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
		if ok := it.MustBool(m.ContainsKey("key")); !ok {
			t.Fatalf("key not found")
		}
		if ok := it.MustBool(m.ContainsValue("value")); !ok {
			t.Fatalf("value not found")
		}
		if value := it.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil!= %v", value)
		}
	})
}

func TestMapRemove(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
		if !it.MustBool(m.ContainsKey("key")) {
			t.Fatalf("key not found")
		}
		if value, err := m.Remove("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target nil != %v", value)
		}
		if it.MustBool(m.ContainsKey("key")) {
			t.Fatalf("key found")
		}
	})
}

func TestGetAll(t *testing.T) {
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
			it.Must(m.Set(pair.Key, pair.Value))
		}
		time.Sleep(1 * time.Second)
		for _, pair := range allPairs {
			it.AssertEquals(t, pair.Value, it.MustValue(m.Get(pair.Key)))
		}
		if kvs, err := m.GetAll(keys...); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, kvs) {
			t.Fatalf("target: %#v != %#v", target, kvs)
		}
	})
}

func TestMapGetKeySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		it.Must(m.Set("k1", "v1"))
		it.Must(m.Set("k2", "v2"))
		it.Must(m.Set("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
		if keys, err := m.GetKeySet(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeStringSet(targetKeySet), makeStringSet(keys)) {
			t.Fatalf("target: %#v != %#v", targetKeySet, keys)
		}
	})
}

func TestMap_GetValues(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValues := []interface{}{"v1", "v2", "v3"}
		it.Must(m.Set("k1", "v1"))
		it.Must(m.Set("k2", "v2"))
		it.Must(m.Set("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
		if values, err := m.GetValues(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeInterfaceSet(targetValues), makeInterfaceSet(values)) {
			t.Fatalf("target: %#v != %#v", targetValues, values)
		}
	})
}

func TestMap_GetValuesWithPredicate(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		targetValues := []interface{}{serialization.JSON(`{"A": 10, "B": 200}`), serialization.JSON(`{"A": 10, "B": 30}`)}
		it.Must(m.Set("k1", serialization.JSON(`{"A": 10, "B": 200}`)))
		it.Must(m.Set("k2", serialization.JSON(`{"A": 10, "B": 30}`)))
		it.Must(m.Set("k3", serialization.JSON(`{"A": 5, "B": 200}`)))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, serialization.JSON(`{"A": 10, "B": 200}`), it.MustValue(m.Get("k1")))
		it.AssertEquals(t, serialization.JSON(`{"A": 10, "B": 30}`), it.MustValue(m.Get("k2")))
		it.AssertEquals(t, serialization.JSON(`{"A": 5, "B": 200}`), it.MustValue(m.Get("k3")))
		if values, err := m.GetValuesWithPredicate(predicate.Equal("A", 10)); err != nil {
			t.Fatal(err)
		} else if len(targetValues) != len(values) {
			t.Fatalf("target len: %d != %d", len(targetValues), len(values))
		}
	})
}

func TestPutAll(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		pairs := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(pairs); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
	})
}

func TestMapGetEntrySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		target := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(target); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if entries, err := m.GetEntrySet(); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestGetEntrySetWithPredicateUsingPortable(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.Map) {
		okValue := "foo-Ğİ"
		noValue := "foo"
		entries := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: okValue, B: 10}),
			types.NewEntry("k2", &it.SamplePortable{A: noValue, B: 15}),
			types.NewEntry("k3", &it.SamplePortable{A: okValue, B: 10}),
		}
		if err := m.PutAll(entries); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: okValue, B: 10}),
			types.NewEntry("k3", &it.SamplePortable{A: okValue, B: 10}),
		}
		if entries, err := m.GetEntrySetWithPredicate(predicate.And(predicate.Equal("A", okValue), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestGetEntrySetWithPredicateUsingJSON(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		entries := []types.Entry{
			types.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.Json()),
			types.NewEntry("k2", it.SamplePortable{A: "foo", B: 15}.Json()),
			types.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.Json()),
		}
		if err := m.PutAll(entries); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []types.Entry{
			types.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.Json()),
			types.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.Json()),
		}
		if entries, err := m.GetEntrySetWithPredicate(predicate.And(predicate.Equal("A", "foo"), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestGetEntryView(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", "v1"))
		if ev, err := m.GetEntryView("k1"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, "k1", ev.Key())
			it.AssertEquals(t, "v1", ev.Value())
		}
	})
}

func TestMap_AddIndexWithConfig(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", serialization.JSON(`{"A": 10, "B": 40}`)))
		indexConfig := types.IndexConfig{
			Name:               "my-index",
			Type:               types.IndexTypeBitmap,
			Attributes:         []string{"A"},
			BitmapIndexOptions: types.BitmapIndexOptions{UniqueKey: "B", UniqueKeyTransformation: types.UniqueKeyTransformationLong},
		}
		if err := m.AddIndexWithConfig(indexConfig); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMapAddIndexValidationError(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		indexConfig := types.IndexConfig{
			Name:               "my-index",
			Type:               types.IndexTypeBitmap,
			Attributes:         []string{"A", "B"},
			BitmapIndexOptions: types.BitmapIndexOptions{UniqueKey: "B", UniqueKeyTransformation: types.UniqueKeyTransformationLong},
		}
		if err := m.AddIndexWithConfig(indexConfig); err == nil {
			t.Fatalf("should have failed")
		} else {
			if _, ok := err.(hz.ValidationError); !ok {
				t.Fatalf("should have returned a validation error")
			}
		}
	})
}

func TestMap_Flush(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", "v1"))
		if err := m.Flush(); err != nil {
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
	it.MapTesterWithConfigBuilderWithName(t, "test-map", nil, func(t *testing.T, m *hz.Map) {
		putSampleKeyValues(m, 2)
		it.Must(m.EvictAll())
		it.Must(m.PutTransient("k0", "new-v0"))
		it.Must(m.PutTransient("k1", "new-v1"))
		time.Sleep(1 * time.Second)
		it.Must(m.LoadAllWithoutReplacing("k0", "k1"))
		targetEntrySet := []types.Entry{
			{Key: "k0", Value: "new-v0"},
			{Key: "k1", Value: "new-v1"},
		}
		entrySet := it.MustValue(m.GetAll("k0", "k1")).([]types.Entry)
		if !entriesEqualUnordered(targetEntrySet, entrySet) {
			t.Fatalf("target %#v != %#v", targetEntrySet, entrySet)
		}
	})
}

func TestMap_LoadAllReplacing(t *testing.T) {
	it.MapTesterWithConfigBuilderWithName(t, "test-map", nil, func(t *testing.T, m *hz.Map) {
		keys := putSampleKeyValues(m, 10)
		it.Must(m.EvictAll())
		it.Must(m.LoadAllReplacing())
		entrySet := it.MustValue(m.GetAll(keys...)).([]types.Entry)
		if len(keys) != len(entrySet) {
			t.Fatalf("target len: %d != %d", len(keys), len(entrySet))
		}
		it.Must(m.EvictAll())
		keys = keys[:5]
		it.Must(m.LoadAllReplacing(keys...))
		entrySet = it.MustValue(m.GetAll(keys...)).([]types.Entry)
		if len(keys) != len(entrySet) {
			t.Fatalf("target len: %d != %d", len(keys), len(entrySet))
		}
	})
}

func TestMap_Lock(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if err := m.Lock("k1"); err != nil {
			t.Fatal(err)
		}
		if locked, err := m.IsLocked("k1"); err != nil {
			log.Fatal(err)
		} else {
			it.AssertEquals(t, true, locked)
		}
		if err := m.Unlock("k1"); err != nil {
			log.Fatal(err)
		}
		if locked, err := m.IsLocked("k1"); err != nil {
			log.Fatal(err)
		} else {
			it.AssertEquals(t, false, locked)
		}
	})
}

func TestMapLockWithContext(t *testing.T) {
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		const mapName = "lock-map"
		m := it.MustValue(client.GetMapContext(context.Background(), mapName)).(*hz.Map)
		defer m.EvictAll()
		it.Must(m.Lock("k1"))
		locked := false
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			m, _ := client.GetMapContext(context.Background(), mapName)
			if locked, err = m.IsLocked("k1"); err != nil {
				panic(err)
			}
		}()
		wg.Wait()
		if !locked {
			t.Fatalf("should be locked")
		}
	})
}

func TestMap_ForceUnlock(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if err := m.Lock("k1"); err != nil {
			t.Fatal(err)
		}
		if locked, err := m.IsLocked("k1"); err != nil {
			log.Fatal(err)
		} else {
			it.AssertEquals(t, true, locked)
		}
		if err := m.ForceUnlock("k1"); err != nil {
			log.Fatal(err)
		}
		if locked, err := m.IsLocked("k1"); err != nil {
			log.Fatal(err)
		} else {
			it.AssertEquals(t, false, locked)
		}
	})
}

func TestMapIsEmptySize(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target: true != false")
		}
		targetSize := 0
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		it.MustValue(m.Put("k1", "v1"))
		it.MustValue(m.Put("k2", "v2"))
		it.MustValue(m.Put("k3", "v3"))
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if value {
			t.Fatalf("target: false != true")
		}
		targetSize = 3
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
	})
}

func TestRemoveAll(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.Map) {
		entries := []types.Entry{
			types.NewEntry("k1", &it.SamplePortable{A: "foo", B: 10}),
			types.NewEntry("k2", &it.SamplePortable{A: "foo", B: 15}),
			types.NewEntry("k3", &it.SamplePortable{A: "foo", B: 10}),
		}
		it.Must(m.PutAll(entries))
		time.Sleep(1 * time.Second)
		if err := m.RemoveAll(predicate.Equal("B", 10)); err != nil {
			t.Fatal(err)
		}
		target := []types.Entry{
			types.NewEntry("k2", &it.SamplePortable{A: "foo", B: 15}),
		}
		if kvs, err := m.GetAll("k1", "k2", "k3"); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, kvs) {
			t.Fatalf("target: %#v != %#v", target, kvs)
		}

	})
}

func TestRemoveIfSame(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", "v1"))
		it.Must(m.Set("k2", "v2"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		if removed, err := m.RemoveIfSame("k1", "v1"); err != nil {
			t.Fatal(err)
		} else if !removed {
			t.Fatalf("not removed")
		}
		it.AssertEquals(t, false, it.MustValue(m.ContainsKey("k1")))
		if removed, err := m.RemoveIfSame("k2", "v1"); err != nil {
			t.Fatal(err)
		} else if removed {
			t.Fatalf("removed")
		}
		it.AssertEquals(t, true, it.MustValue(m.ContainsKey("k2")))
	})
}

func TestReplace(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", "v1"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		if oldValue, err := m.Replace("k1", "v2"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, "v1", oldValue)
		}
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k1")))
	})
}

func TestReplaceIfSame(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set("k1", "v1"))
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		if replaced, err := m.ReplaceIfSame("k1", "v1", "v2"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, true, replaced)
		}
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k1")))
	})
}

func TestMapEntryNotifiedEvent(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		time.Sleep(1 * time.Second)
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
		}
		listenerConfig.NotifyEntryAdded(true)
		listenerConfig.NotifyEntryAdded(true)
		subscriptionID, err := m.AddEntryListener(listenerConfig, handler)
		if err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", "v1"))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		if err := m.RemoveEntryListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", "v1"))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestMapEntryNotifiedEventToKey(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		time.Sleep(1 * time.Second)
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Key:          "k1",
		}
		listenerConfig.NotifyEntryAdded(true)
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.Must(m.Set("k1", "v1"))
		assert.Equal(t, "v1", it.MustValue(m.Get("k1")))
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k2", "v1"))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestMapEntryNotifiedEventWithPredicate(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.Map) {
		time.Sleep(1 * time.Second)
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Predicate:    predicate.Equal("A", "foo"),
		}
		listenerConfig.NotifyEntryAdded(true)
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestMapEntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.Map) {
		time.Sleep(1 * time.Second)
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		listenerConfig := hz.MapEntryListenerConfig{
			IncludeValue: true,
			Key:          "k1",
			Predicate:    predicate.Equal("A", "foo"),
		}
		listenerConfig.NotifyEntryAdded(true)
		listenerConfig.NotifyEntryAdded(true)
		if _, err := m.AddEntryListener(listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k2", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestMap_Destroy(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if err := m.Destroy(); err != nil {
			t.Fatal(err)
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

func makeInterfaceSet(items []interface{}) map[interface{}]struct{} {
	result := map[interface{}]struct{}{}
	for _, item := range items {
		result[item] = struct{}{}
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
		it.MustValue(m.Put(key, value))
		keys = append(keys, key)
	}
	return keys
}
