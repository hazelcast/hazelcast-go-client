// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _map

import (
	"github.com/hazelcast/hazelcast-go-client"
	. "github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

var mp IMap
var mp2 IMap
var client hazelcast.IHazelcastInstance

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	mp, _ = client.GetMap("myMap")
	predicateTestInit()
	m.Run()
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestMapProxy_Name(t *testing.T) {
	name := "myMap"
	if name != mp.Name() {
		t.Errorf("Name() failed")
	}
}

func TestMapProxy_ServiceName(t *testing.T) {
	serviceName := common.SERVICE_NAME_MAP
	if serviceName != mp.ServiceName() {
		t.Errorf("ServiceName() failed")
	}
}

func TestMapProxy_PartitionKey(t *testing.T) {
	name := "myMap"
	if name != mp.PartitionKey() {
		t.Errorf("PartitionKey() failed")
	}
}

func TestMapProxy_SinglePutGet(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_SinglePutGetInt(t *testing.T) {
	testKey := 1
	testValue := 25
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, int64(testValue), "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_PutWithNilKey(t *testing.T) {
	testValue := "testingValue"
	_, err := mp.Put(nil, testValue)
	AssertErrorNotNil(t, err, "put did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutWithNilValue(t *testing.T) {
	testKey := "testingKey"
	_, err := mp.Put(testKey, nil)
	AssertErrorNotNil(t, err, "put did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_GetWithNilKey(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	_, err := mp.Get(nil)
	AssertErrorNotNil(t, err, "get did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryRemove(t *testing.T) {
	mp.Put("testKey", "testValue")
	_, err := mp.TryRemove("testKey", 5, time.Second)
	if err != nil {
		t.Fatal("tryRemove failed ", err)
	}
	mp.Clear()
}

func TestMapProxy_TryRemoveWithNilKey(t *testing.T) {
	_, err := mp.TryRemove(nil, 1, time.Second)
	AssertErrorNotNil(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryPut(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.TryPut(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_TryPutWithNilKey(t *testing.T) {
	testValue := "testingValue"
	_, err := mp.TryPut(nil, testValue)
	AssertErrorNotNil(t, err, "tryPut did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryPutWithNilValue(t *testing.T) {
	testKey := "testingKey"
	_, err := mp.TryPut(testKey, nil)
	AssertErrorNotNil(t, err, "tryPut did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_ManyPutGet(t *testing.T) {
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
		res, err := mp.Get(testKey)
		AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	}
	mp.Clear()
}

func TestMapProxy_Remove(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.Remove(testKey)
	AssertEqualf(t, err, removed, testValue, "remove returned a wrong value")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(0), "map size should be 0.")
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_RemoveWithNilKey(t *testing.T) {
	_, err := mp.Remove(nil)
	AssertErrorNotNil(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_RemoveAll(t *testing.T) {
	var testMap map[interface{}]interface{} = make(map[interface{}]interface{}, 41)
	for i := 0; i < 50; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), int32(i))
		if i < 41 {
			testMap["testingKey"+strconv.Itoa(i)] = int32(i)
		}
	}
	mp.RemoveAll(GreaterThan("this", int32(40)))
	entryList, _ := mp.EntrySet()
	if len(testMap) != len(entryList) {
		t.Fatalf("map RemoveAll failed")
	}
	for _, pair := range entryList {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := testMap[key]
		if !found || expectedValue != value {
			t.Fatalf("map RemoveAll failed")
		}
	}
	mp.Clear()
}

func TestMapProxy_RemoveAllWithNilPredicate(t *testing.T) {
	err := mp.RemoveAll(nil)
	AssertErrorNotNil(t, err, "removeAll did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_RemoveIfSame(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.RemoveIfSame(testKey, "testinValue1")
	AssertEqualf(t, err, removed, false, "removeIfSame returned a wrong value")
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, true, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNilKey(t *testing.T) {
	_, err := mp.RemoveIfSame(nil, "test")
	AssertErrorNotNil(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNilValue(t *testing.T) {
	_, err := mp.RemoveIfSame("test", nil)
	AssertErrorNotNil(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutTransient(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 100, time.Second)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, "nextValue", "putTransient failed")
	mp.Clear()

}

func TestMapProxy_PutTransientWhenExpire(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 1, time.Millisecond)
	time.Sleep(5 * time.Second)
	res, err := mp.Get(testKey)
	AssertNilf(t, err, res, "putTransient failed")
	mp.Clear()

}

func TestMapProxy_PutTransientWithNilKey(t *testing.T) {
	testValue := "testingValue"
	err := mp.PutTransient(nil, testValue, 1, time.Millisecond)
	AssertErrorNotNil(t, err, "putTransient did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNilValue(t *testing.T) {
	testKey := "testingKey"
	err := mp.PutTransient(testKey, nil, 1, time.Millisecond)
	AssertErrorNotNil(t, err, "putTransient did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_ContainsKey(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, true, "containsKey returned a wrong result")
	found, err = mp.ContainsKey("testingKey2")
	AssertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_ContainsKeyWithNilKey(t *testing.T) {
	_, err := mp.ContainsKey(nil)
	AssertErrorNotNil(t, err, "containsKey did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ContainsValue(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsValue(testValue)
	AssertEqualf(t, err, found, true, "containsValue returned a wrong result")
	found, err = mp.ContainsValue("testingValue2")
	AssertEqualf(t, err, found, false, "containsValue returned a wrong result")
	mp.Clear()
}

func TestMapProxy_ContainsValueWithNilValue(t *testing.T) {
	_, err := mp.ContainsValue(nil)
	AssertErrorNotNil(t, err, "containsValue did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_Clear(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	err := mp.Clear()
	if err != nil {
		t.Fatal(err)
	} else {
		size, err := mp.Size()
		AssertEqualf(t, err, size, int32(0), "Map clear failed.")
	}
}

func TestMapProxy_Delete(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Delete("testingKey1")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(9), "Map Delete failed")
	mp.Clear()
}

func TestMapProxy_DeleteWithNilKey(t *testing.T) {
	err := mp.Delete(nil)
	AssertErrorNotNil(t, err, "delete did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_IsEmpty(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	empty, err := mp.IsEmpty()
	AssertEqualf(t, err, empty, false, "Map IsEmpty returned a wrong value")
	mp.Clear()
}

func TestMapProxy_Evict(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Evict("testingKey1")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(9), "Map evict failed.")
	found, err := mp.ContainsKey("testingKey1")
	AssertEqualf(t, err, found, false, "Map evict failed.")
}

func TestMapProxy_EvictWithNilKey(t *testing.T) {
	_, err := mp.Evict(nil)
	AssertErrorNotNil(t, err, "evict did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_EvictAll(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.EvictAll()
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(0), "Map evict failed.")
}

func TestMapProxy_Flush(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	err := mp.Flush()
	if err != nil {
		t.Fatal(err)
	}
	mp.Clear()
}

func TestMapProxy_IsLocked(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	err = mp.Lock("testingKey")
	if err != nil {
		t.Fatal(err)
	}
	locked, err = mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, true, "Key should be locked.")
	err = mp.Unlock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")

}

func TestMapProxy_IsLockedWithNilKey(t *testing.T) {
	_, err := mp.IsLocked(nil)
	AssertErrorNotNil(t, err, "isLocked did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_UnlockWithNilKey(t *testing.T) {
	err := mp.Unlock(nil)
	AssertErrorNotNil(t, err, "unlock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_LockWithLeaseTime(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	mp.LockWithLeaseTime("testingKey", 10, time.Millisecond)
	time.Sleep(5 * time.Second)
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
}

func TestMapProxy_LocktWithNilKey(t *testing.T) {
	err := mp.Lock(nil)
	AssertErrorNotNil(t, err, "lock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryLock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1, time.Second, 2, time.Second)
	AssertEqualf(t, err, ok, true, "Try Lock failed")
	time.Sleep(5 * time.Second)
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	mp.ForceUnlock("testingKey")
	mp.Clear()
}

func TestMapProxy_ForceUnlockWithNilKey(t *testing.T) {
	err := mp.ForceUnlock(nil)
	AssertErrorNotNil(t, err, "forceUnlock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryLockWithNilKey(t *testing.T) {
	_, err := mp.TryLock(nil)
	AssertErrorNotNil(t, err, "tryLock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ForceUnlock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1, time.Second, 20, time.Second)
	AssertEqualf(t, err, ok, true, "Try Lock failed")
	mp.ForceUnlock("testingKey")
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	mp.Unlock("testingKey")
	mp.Clear()
}

func TestMapProxy_Replace(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.Replace("testingKey1", "testingValue2")
	AssertEqualf(t, err, replaced, "testingValue1", "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue2", "Map Replace failed.")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNilKey(t *testing.T) {
	_, err := mp.Replace(nil, "test")
	AssertErrorNotNil(t, err, "replace did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNilValue(t *testing.T) {
	_, err := mp.Replace("test", nil)
	AssertErrorNotNil(t, err, "replace did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_Size(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(10), "Map size returned a wrong value")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSame(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue1", "testingValue2")
	AssertEqualf(t, err, replaced, true, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue2", "Map ReplaceIfSame failed.")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilKey(t *testing.T) {
	_, err := mp.ReplaceIfSame(nil, "test", "test")
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilOldValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", nil, "test")
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nil oldValue")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilNewValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", "test", nil)
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nil newValue")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWhenDifferent(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue3", "testingValue2")
	AssertEqualf(t, err, replaced, false, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map ReplaceIfSame failed.")
	mp.Clear()
}

func TestMapProxy_Set(t *testing.T) {
	err := mp.Set("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_SetWithNilKey(t *testing.T) {
	err := mp.Set(nil, "test")
	AssertErrorNotNil(t, err, "Set did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_SetWithNilValue(t *testing.T) {
	err := mp.Set("test", nil)
	AssertErrorNotNil(t, err, "set did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_SetWithTtl(t *testing.T) {
	err := mp.SetWithTtl("testingKey1", "testingValue1", 0, time.Second)
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map SetWithTtl failed.")
	mp.SetWithTtl("testingKey1", "testingValue2", 1, time.Millisecond)
	time.Sleep(5 * time.Second)
	newValue, err = mp.Get("testingKey1")
	AssertNilf(t, err, newValue, "Map SetWithTtl failed.")
	mp.Clear()
}

func TestMapProxy_PutIfAbsent(t *testing.T) {
	_, err := mp.PutIfAbsent("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNilKey(t *testing.T) {
	_, err := mp.PutIfAbsent(nil, "test")
	AssertErrorNotNil(t, err, "putIfAbsent did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNilValue(t *testing.T) {
	_, err := mp.PutIfAbsent("test", nil)
	AssertErrorNotNil(t, err, "putIfAbsent did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_PutAll(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := mp.PutAll(testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		entryList, err := mp.EntrySet()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range entryList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := testMap[key]
			if !found || expectedValue != value {
				t.Fatalf("Map PutAll failed")
			}
		}
	}
	mp.Clear()
}

func TestMapProxy_PutAllWithNilMap(t *testing.T) {
	err := mp.PutAll(nil)
	AssertErrorNotNil(t, err, "putAll did not return an error for nil map")
	mp.Clear()
}

func TestMapProxy_KeySet(t *testing.T) {
	var expecteds []string = make([]string, 10)
	var ret []string = make([]string, 10)
	for i := 0; i < 10; i++ {
		mp.Put(strconv.Itoa(i), int32(i))
		expecteds[i] = strconv.Itoa(i)
	}
	keySet, _ := mp.KeySet()
	for j := 0; j < 10; j++ {
		ret[j] = keySet[j].(string)
	}
	sort.Strings(ret)
	if len(keySet) != len(expecteds) || !reflect.DeepEqual(ret, expecteds) {
		t.Fatalf("map KeySet failed")
	}
}

func TestMapProxy_KeySetWihPredicate(t *testing.T) {
	expected := "5"
	for i := 0; i < 10; i++ {
		mp.Put(strconv.Itoa(i), int32(i))
	}
	keySet, _ := mp.KeySetWithPredicate(Equal("this", "5"))
	if len(keySet) != 1 || keySet[0].(string) != expected {
		t.Fatalf("map KeySetWithPredicate failed")
	}
}

func TestMapProxy_KeySetWithPredicateWithNilPredicate(t *testing.T) {
	_, err := mp.KeySetWithPredicate(nil)
	AssertErrorNotNil(t, err, "keySetWithPredicate did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_Values(t *testing.T) {
	var expecteds []string = make([]string, 10)
	var ret []string = make([]string, 10)
	for i := 0; i < 10; i++ {
		mp.Put(strconv.Itoa(i), strconv.Itoa(i))
		expecteds[i] = strconv.Itoa(i)
	}
	values, _ := mp.Values()
	for j := 0; j < 10; j++ {
		ret[j] = values[j].(string)
	}
	sort.Strings(ret)
	if len(values) != len(expecteds) || !reflect.DeepEqual(ret, expecteds) {
		t.Fatalf("map Values failed")
	}
}

func TestMapProxy_ValuesWithPredicate(t *testing.T) {
	expected := "5"
	for i := 0; i < 10; i++ {
		mp.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	values, _ := mp.ValuesWithPredicate(Equal("this", "5"))
	if len(values) != 1 || values[0].(string) != expected {
		t.Fatalf("map ValuesWithPredicate failed")
	}
}

func TestMapProxy_ValuesWithPredicateWithNilPredicate(t *testing.T) {
	_, err := mp.ValuesWithPredicate(nil)
	AssertErrorNotNil(t, err, "ValuesWithPredicate did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_EntrySetWithPredicate(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	searchedMap := make(map[interface{}]interface{})
	values := []string{"value1", "wantedValue", "wantedValue", "value2", "value3", "wantedValue", "wantedValue", "value4", "value5", "wantedValue"}
	searchedMap["testingKey1"] = "wantedValue"
	searchedMap["testingKey2"] = "wantedValue"
	searchedMap["testingKey5"] = "wantedValue"
	searchedMap["testingKey6"] = "wantedValue"
	searchedMap["testingKey9"] = "wantedValue"
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = values[i]
	}
	err := mp.PutAll(testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		entryList, err := mp.EntrySetWithPredicate(Sql("this == wantedValue"))
		if err != nil {
			t.Fatal(err)
		}
		if len(searchedMap) != len(entryList) {
			t.Fatalf("map EntrySetWithPredicate() failed")
		}
		for _, pair := range entryList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := searchedMap[key]
			if !found || expectedValue != value {
				t.Fatalf("map EntrySetWithPredicate() failed")
			}
		}
	}
	mp.Clear()
}

func TestMapProxy_GetAll(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := mp.PutAll(testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		keys := make([]interface{}, 0)
		for k, _ := range testMap {
			keys = append(keys, k)
		}
		valueMap, err := mp.GetAll(keys)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range valueMap {
			expectedValue, found := testMap[key]
			if !found || expectedValue != value {
				t.Fatalf("Map GetAll failed")
			}
		}

	}
	mp.Clear()
}

func TestMapProxy_GetAllWithNilKeys(t *testing.T) {
	_, err := mp.GetAll(nil)
	AssertErrorNotNil(t, err, "GetAll did not return an error for nil keys")
	mp.Clear()
}

func TestMapProxy_AddIndex(t *testing.T) {
	mp2, _ := client.GetMap("mp2")
	err := mp2.AddIndex("age", true)
	if err != nil {
		t.Fatal("addIndex failed")
	}
	mp2.Clear()
}

func TestMapProxy_GetEntryView(t *testing.T) {
	mp.Put("key", "value")
	mp.Get("key")
	mp.Put("key", "newValue")

	entryView, err := mp.GetEntryView("key")
	AssertEqualf(t, err, entryView.Key(), "key", "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.Value(), "newValue", "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.Hits(), int64(2), "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.EvictionCriteriaNumber(), int64(0), "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.Version(), int64(1), "Map GetEntryView returned a wrong view.")
	if cost := entryView.Cost(); cost <= 0 {
		t.Fatal("entryView cost should be greater than 0.")
	}
	if creationTime := entryView.CreationTime(); creationTime <= 0 {
		t.Fatal("entryView creationTime should be greater than 0.")
	}
	if expirationTime := entryView.ExpirationTime(); expirationTime <= 0 {
		t.Fatal("entryView expirationTime should be greater than 0.")
	}
	if lastAccessTime := entryView.LastAccessTime(); lastAccessTime <= 0 {
		t.Fatal("entryView lastAccessTime should be greater than 0.")
	}
	if lastUpdateTime := entryView.LastUpdateTime(); lastUpdateTime <= 0 {
		t.Fatal("entryView lastUpdateTime should be greater than 0.")
	}
	if ttl := entryView.Ttl(); ttl <= 0 {
		t.Fatal("entryView ttl should be greater than 0.")
	}
	AssertEqualf(t, err, entryView.LastStoredTime(), int64(0), "Map GetEntryView returned a wrong view.")
	mp.Clear()
}

func TestMapProxy_GetEntryViewWithNilKey(t *testing.T) {
	_, err := mp.GetEntryView(nil)
	AssertErrorNotNil(t, err, "GetEntryView did not return an error for nil key")
	mp.Clear()
}

type AddEntry struct {
	wg       *sync.WaitGroup
	event    IEntryEvent
	mapEvent IMapEvent
}

func (addEntry *AddEntry) EntryAdded(event IEntryEvent) {
	addEntry.event = event
	addEntry.wg.Done()
}

func (addEntry *AddEntry) EntryUpdated(event IEntryEvent) {
	addEntry.wg.Done()
}

func (addEntry *AddEntry) EntryRemoved(event IEntryEvent) {
	addEntry.wg.Done()
}

func (addEntry *AddEntry) EntryEvicted(event IEntryEvent) {
	addEntry.wg.Done()
}

func (addEntry *AddEntry) EntryEvictAll(event IMapEvent) {
	addEntry.mapEvent = event
	addEntry.wg.Done()
}

func (addEntry *AddEntry) EntryClearAll(event IMapEvent) {
	addEntry.wg.Done()
}

func TestMapProxy_AddEntryListenerAdded(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryAdded.event.Key(), "key123", "AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryAdded.event.Value(), "value", "AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryAdded.event.OldValue(), nil, "AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryAdded.event.MergingValue(), nil, "AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryAdded.event.EventType(), int32(1), "AddEntryListener entryAdded failed")

	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerUpdated(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("key1", "value")
	mp.Put("key1", "value")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryUpdated failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerEvicted(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Evict("test")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryEvicted failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerRemoved(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Remove("test")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryRemoved failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerEvictAll(t *testing.T) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.EvictAll()
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryEvictAll failed")
	AssertEqualf(t, nil, entryAdded.mapEvent.EventType(), int32(16), "AddEntryListener entryEvictAll failed")
	AssertEqualf(t, nil, entryAdded.mapEvent.NumberOfAffectedEntries(), int32(1), "AddEntryListener entryEvictAll failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerClear(t *testing.T) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Clear()
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryClear failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerWithPredicate(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerWithPredicate(entryAdded, Equal("this", "value"), true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListenerWithPredicate failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKey(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerToKey(entryAdded, "key1", true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key1", "value1")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListenerToKey failed")
	wg.Add(1)
	mp.Put("key2", "value1")
	timeout = WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "AddEntryListenerToKey failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithPredicate(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerToKeyWithPredicate(entryAdded, Equal("this", "value1"), "key1", true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key1", "value1")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListenerToKeyWithPredicate failed")
	wg.Add(1)
	mp.Put("key1", "value2")
	timeout = WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "AddEntryListenerToKeyWithPredicate failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_RemoveEntryListenerToKeyWithInvalidRegistrationId(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerToKey(entryAdded, "key1", true)
	AssertEqual(t, err, nil, nil)
	invalidRegistrationId := "invalid"
	removed, _ := mp.RemoveEntryListener(&invalidRegistrationId)
	if removed {
		t.Fatal("remove entry listener to key with invalid registration id failed")
	}
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_ExecuteOnKey(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	testKey := "testingKey1"
	testValue := "testingValue"
	mp2.Put(testKey, testValue)
	value, err := mp2.ExecuteOnKey(testKey, processor)
	AssertEqualf(t, err, value, expectedValue, "ExecuteOnKey failed.")
	newValue, err := mp2.Get("testingKey1")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKey failed")
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnKeys(t *testing.T) {

	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp2.Put(testKey, testValue)
	}
	keys := make([]interface{}, 2)
	keys[0] = "testingKey1"
	keys[1] = "testingKey2"
	result, err := mp2.ExecuteOnKeys(keys, processor)
	AssertEqualf(t, err, len(result), 2, "ExecuteOnKeys failed.")
	newValue, err := mp2.Get("testingKey1")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKeys failed")
	newValue, err = mp2.Get("testingKey2")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKeys failed")
	mp2.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnEntries(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp2.Put(testKey, testValue)
	}
	result, err := mp2.ExecuteOnEntries(processor)
	for _, pair := range result {
		AssertEqualf(t, err, pair.Value(), expectedValue, "ExecuteOnEntries failed")
		newValue, err := mp2.Get(pair.Key())
		AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnEntries failed")
	}
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnEntriesWithPredicate(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := int32(i)
		mp2.Put(testKey, testValue)
	}
	result, err := mp2.ExecuteOnEntriesWithPredicate(processor, GreaterThan("this", int32(6)))
	if len(result) != 3 {
		t.Fatal("ExecuteOnEntriesWithPredicate failed")
	}
	for _, pair := range result {
		AssertEqualf(t, err, pair.Value(), expectedValue, "ExecuteOnEntriesWithPredicate failed")
		newValue, err := mp2.Get(pair.Key())
		AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnEntriesWithPredicate failed")
	}
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnKeyWithNonRegisteredProcessor(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 68)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	testKey := "testingKey1"
	testValue := "testingValue"
	mp2.Put(testKey, testValue)
	_, err := mp2.ExecuteOnKey(testKey, processor)
	AssertErrorNotNil(t, err, "non registered processor should return an error")
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_Destroy(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.Destroy()
	mp, _ := client.GetMap("myMap")
	res, err := mp.Get(testKey)
	AssertNilf(t, err, res, "get returned a wrong value")
}

type simpleEntryProcessor struct {
	classId           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor(value string, factoryId int32) *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classId: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryId: factoryId, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryId            int32
}

func (identifiedFactory *identifiedFactory) Create(id int32) IdentifiedDataSerializable {
	if id == identifiedFactory.simpleEntryProcessor.classId {
		return &simpleEntryProcessor{classId: 1}
	} else {
		return nil
	}
}

func (simpleEntryProcessor *simpleEntryProcessor) ReadData(input DataInput) error {
	var err error
	simpleEntryProcessor.value, err = input.ReadUTF()
	return err
}

func (simpleEntryProcessor *simpleEntryProcessor) WriteData(output DataOutput) error {
	output.WriteUTF(simpleEntryProcessor.value)
	return nil
}

func (simpleEntryProcessor *simpleEntryProcessor) FactoryId() int32 {
	return simpleEntryProcessor.identifiedFactory.factoryId
}

func (simpleEntryProcessor *simpleEntryProcessor) ClassId() int32 {
	return simpleEntryProcessor.classId
}

// Serialization error checks

type student struct {
	age int32
}

func TestMapProxy_PutWithNonSerializableKey(t *testing.T) {
	_, err := mp.Put(student{}, "test")
	AssertErrorNotNil(t, err, "put did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutWithNonSerializableValue(t *testing.T) {
	_, err := mp.Put("test", student{})
	AssertErrorNotNil(t, err, "put did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_TryPutWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryPut(student{}, "test")
	AssertErrorNotNil(t, err, "tryPut did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_TryPutWithNonSerializableValue(t *testing.T) {
	_, err := mp.TryPut("test", student{})
	AssertErrorNotNil(t, err, "tryPut did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNonSerializableKey(t *testing.T) {
	err := mp.PutTransient(student{}, "test", 1, time.Second)
	AssertErrorNotNil(t, err, "putTransient did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNonSerializableValue(t *testing.T) {
	err := mp.PutTransient("test", student{}, 1, time.Second)
	AssertErrorNotNil(t, err, "putTransient did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_GetWithNonSerializableKey(t *testing.T) {
	_, err := mp.Get(student{})
	AssertErrorNotNil(t, err, "get did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNonSerializableKey(t *testing.T) {
	_, err := mp.RemoveIfSame(student{}, "test")
	AssertErrorNotNil(t, err, "removeIfSame did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNonSerializableValue(t *testing.T) {
	_, err := mp.RemoveIfSame("test", student{})
	AssertErrorNotNil(t, err, "removeIfSame did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_TryRemoveWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryRemove(student{}, 1, time.Second)
	AssertErrorNotNil(t, err, "tryRemove did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ContainsKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.ContainsKey(student{})
	AssertErrorNotNil(t, err, "containsKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ContainsValueWithNonSerializableValue(t *testing.T) {
	_, err := mp.ContainsValue(student{})
	AssertErrorNotNil(t, err, "containsValue did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_DeleteWithNonSerializableKey(t *testing.T) {
	err := mp.Delete(student{})
	AssertErrorNotNil(t, err, "delete did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_EvictWithNonSerializableKey(t *testing.T) {
	_, err := mp.Evict(student{})
	AssertErrorNotNil(t, err, "evict did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_LockWithNonSerializableKey(t *testing.T) {
	err := mp.Lock(student{})
	AssertErrorNotNil(t, err, "lock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_TryLockWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryLock(student{})
	AssertErrorNotNil(t, err, "tryLock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_UnlockWithNonSerializableKey(t *testing.T) {
	err := mp.Unlock(student{})
	AssertErrorNotNil(t, err, "unlock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ForceUnlockWithNonSerializableKey(t *testing.T) {
	err := mp.ForceUnlock(student{})
	AssertErrorNotNil(t, err, "forceUnlock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_IsLockedWithNonSerializableKey(t *testing.T) {
	_, err := mp.IsLocked(student{})
	AssertErrorNotNil(t, err, "isLocked did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableKey(t *testing.T) {
	_, err := mp.Replace(student{}, "test")
	AssertErrorNotNil(t, err, "replace did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableValue(t *testing.T) {
	_, err := mp.Replace("test", student{})
	AssertErrorNotNil(t, err, "replace did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNonSerializableKey(t *testing.T) {
	_, err := mp.ReplaceIfSame(student{}, "test", "test")
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableOldValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", student{}, "test")
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nonserializable oldValue")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableNewValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", "test", student{})
	AssertErrorNotNil(t, err, "replaceIfSame did not return an error for nonserializable newValue")
	mp.Clear()
}

func TestMapProxy_SetWithNonSerializableKey(t *testing.T) {
	err := mp.Set(student{}, "test")
	AssertErrorNotNil(t, err, "set did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_SetWithNonSerializableValue(t *testing.T) {
	err := mp.Set("test", student{})
	AssertErrorNotNil(t, err, "set did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNonSerializableKey(t *testing.T) {
	_, err := mp.PutIfAbsent(student{}, "test")
	AssertErrorNotNil(t, err, "putIfAbsent did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNonSerializableValue(t *testing.T) {
	_, err := mp.PutIfAbsent("test", student{})
	AssertErrorNotNil(t, err, "putIfAbsent did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutAllWithNonSerializableMapKey(t *testing.T) {
	testMap := make(map[interface{}]interface{}, 0)
	testMap[student{}] = 5
	err := mp.PutAll(testMap)
	AssertErrorNotNil(t, err, "putAll did not return an error for nonserializable map key")
	mp.Clear()
}

func TestMapProxy_PutAllWithNonSerializableMapValue(t *testing.T) {
	testMap := make(map[interface{}]interface{}, 0)
	testMap[5] = student{}
	err := mp.PutAll(testMap)
	AssertErrorNotNil(t, err, "putAll did not return an error for nonserializable map value")
	mp.Clear()
}

func TestMapProxy_GetAllWithNonSerializableKey(t *testing.T) {
	testSlice := make([]interface{}, 1)
	testSlice[0] = student{}
	_, err := mp.GetAll(testSlice)
	AssertErrorNotNil(t, err, "getAll did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_GetEntryViewWithNonSerializableKey(t *testing.T) {
	_, err := mp.GetEntryView(student{})
	AssertErrorNotNil(t, err, "getEntryView did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.AddEntryListenerToKey(nil, student{}, false)
	AssertErrorNotNil(t, err, "addEntryListenerToKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithPredicateWithNonSerializableKey(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(nil, nil, student{}, false)
	AssertErrorNotNil(t, err, "addEntryListenerToKeyWithPredicate did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.ExecuteOnKey(student{}, nil)
	AssertErrorNotNil(t, err, "executeOnKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnEntriesWithNonSerializableKey(t *testing.T) {
	_, err := mp.ExecuteOnEntries(student{})
	AssertErrorNotNil(t, err, "executeOnEntries did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeysWithNonSerializableKey(t *testing.T) {
	testSlice := make([]interface{}, 1)
	testSlice[0] = student{}
	_, err := mp.ExecuteOnKeys(testSlice, nil)
	AssertErrorNotNil(t, err, "executeOnKeys did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeysWithNonSerializableProcessor(t *testing.T) {

	_, err := mp.ExecuteOnKeys(nil, student{})
	AssertErrorNotNil(t, err, "executeOnKeys did not return an error for nonserializable processor")
	mp.Clear()
}
