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

package map1

import (
	"log"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

var mp core.Map
var mp2 core.Map
var client hazelcast.Client
var member *rc.Member

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}

	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	member, _ = remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.SerializationConfig().AddPortableFactory(666, &portableFactory{})
	client, _ = hazelcast.NewClientWithConfig(config)
	mp, _ = client.GetMap("myMap")
	predicateTestInit()
	projectionTestInit()
	m.Run()
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestMapProxy_Name(t *testing.T) {
	name := "myMap"
	if name != mp.Name() {
		t.Error("Name() failed")
	}
}

func TestMapProxy_ServiceName(t *testing.T) {
	serviceName := bufutil.ServiceNameMap
	if serviceName != mp.ServiceName() {
		t.Error("ServiceName() failed")
	}
}

func TestMapProxy_PartitionKey(t *testing.T) {
	name := "myMap"
	if name != mp.PartitionKey() {
		t.Error("PartitionKey() failed")
	}
}

func TestMapProxy_SinglePutGet(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	require.NoError(t, err)
	assert.Equalf(t, res, testValue, "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_SinglePutGetInt(t *testing.T) {
	testKey := 1
	testValue := 25
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	require.NoError(t, err)
	assert.Equalf(t, res, int64(testValue), "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_PutWithNilKey(t *testing.T) {
	testValue := "testingValue"
	_, err := mp.Put(nil, testValue)
	require.Errorf(t, err, "put did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutWithNilValue(t *testing.T) {
	testKey := "testingKey"
	_, err := mp.Put(testKey, nil)
	require.Errorf(t, err, "put did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_GetWithNilKey(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	_, err := mp.Get(nil)
	require.Errorf(t, err, "get did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryRemove(t *testing.T) {
	mp.Put("testKey", "testValue")
	_, err := mp.TryRemove("testKey", 5*time.Second)
	if err != nil {
		t.Fatal("tryRemove failed ", err)
	}
	mp.Clear()
}

func TestMapProxy_TryRemoveWithNilKey(t *testing.T) {
	_, err := mp.TryRemove(nil, 1*time.Second)
	require.Errorf(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryPut(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.TryPut(testKey, testValue)
	res, err := mp.Get(testKey)
	require.NoError(t, err)
	assert.Equalf(t, res, testValue, "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_TryPutWithTimeout(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	ok, err := mp.TryPutWithTimeout(testKey, testValue, 1*time.Second)
	require.NoError(t, err)
	assert.Equal(t, ok, true)
	mp.Clear()
}

func TestMapProxy_TryPutWithTimeoutWithNilKey(t *testing.T) {
	testValue := "testingValue"
	_, err := mp.TryPutWithTimeout(nil, testValue, 1*time.Second)
	require.Errorf(t, err, "Map.TryPutWithTimeout should return error for nil key")
	mp.Clear()
}

func TestMapProxy_TryPutWithNilKey(t *testing.T) {
	testValue := "testingValue"
	_, err := mp.TryPut(nil, testValue)
	require.Errorf(t, err, "tryPut did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryPutWithNilValue(t *testing.T) {
	testKey := "testingKey"
	_, err := mp.TryPut(testKey, nil)
	require.Errorf(t, err, "tryPut did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_ManyPutGet(t *testing.T) {
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
		res, err := mp.Get(testKey)
		require.NoError(t, err)
		assert.Equalf(t, res, testValue, "get returned a wrong value")
	}
	mp.Clear()
}

func TestMapProxy_Remove(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.Remove(testKey)
	require.NoError(t, err)
	assert.Equalf(t, removed, testValue, "remove returned a wrong value")
	size, err := mp.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "map size should be 0.")
	found, err := mp.ContainsKey(testKey)
	require.NoError(t, err)
	assert.Equalf(t, found, false, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_RemoveWithNilKey(t *testing.T) {
	_, err := mp.Remove(nil)
	require.Errorf(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_RemoveAll(t *testing.T) {
	var testMap = make(map[interface{}]interface{}, 41)
	for i := 0; i < 50; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), int32(i))
		if i < 41 {
			testMap["testingKey"+strconv.Itoa(i)] = int32(i)
		}
	}
	mp.RemoveAll(predicate.GreaterThan("this", int32(40)))
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
	require.Errorf(t, err, "removeAll did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_RemoveIfSame(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.RemoveIfSame(testKey, "testinValue1")
	require.NoError(t, err)
	assert.Equalf(t, removed, false, "removeIfSame returned a wrong value")
	found, err := mp.ContainsKey(testKey)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNilKey(t *testing.T) {
	_, err := mp.RemoveIfSame(nil, "test")
	require.Errorf(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNilValue(t *testing.T) {
	_, err := mp.RemoveIfSame("test", nil)
	require.Errorf(t, err, "remove did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutTransient(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 100*time.Second)
	res, err := mp.Get(testKey)
	require.NoError(t, err)
	assert.Equalf(t, res, "nextValue", "putTransient failed")
	mp.Clear()

}

func TestMapProxy_PutTransientWhenExpire(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 1*time.Millisecond)
	test.AssertTrueEventually(t, func() bool {
		res, err := mp.Get(testKey)
		return err == nil && res == nil
	})
	mp.Clear()
}

func TestMapProxy_PutTransientWithNilKey(t *testing.T) {
	testValue := "testingValue"
	err := mp.PutTransient(nil, testValue, 1*time.Millisecond)
	require.Errorf(t, err, "putTransient did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNilValue(t *testing.T) {
	testKey := "testingKey"
	err := mp.PutTransient(testKey, nil, 1*time.Millisecond)
	require.Errorf(t, err, "putTransient did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_ContainsKey(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsKey(testKey)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "containsKey returned a wrong result")
	found, err = mp.ContainsKey("testingKey2")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "containsKey returned a wrong result")
	mp.Clear()
}

func TestMapProxy_ContainsKeyWithNilKey(t *testing.T) {
	_, err := mp.ContainsKey(nil)
	require.Errorf(t, err, "containsKey did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ContainsValue(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsValue(testValue)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "containsValue returned a wrong result")
	found, err = mp.ContainsValue("testingValue2")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "containsValue returned a wrong result")
	mp.Clear()
}

func TestMapProxy_ContainsValueWithNilValue(t *testing.T) {
	_, err := mp.ContainsValue(nil)
	require.Errorf(t, err, "containsValue did not return an error for nil value")
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
		require.NoError(t, err)
		assert.Equalf(t, size, int32(0), "Map clear failed.")
	}
}

func TestMapProxy_Delete(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Delete("testingKey1")
	size, err := mp.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(9), "Map Delete failed")
	mp.Clear()
}

func TestMapProxy_DeleteWithNilKey(t *testing.T) {
	err := mp.Delete(nil)
	require.Errorf(t, err, "delete did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_IsEmpty(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	empty, err := mp.IsEmpty()
	require.NoError(t, err)
	assert.Equalf(t, empty, false, "Map IsEmpty returned a wrong value")
	mp.Clear()
}

func TestMapProxy_Evict(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Evict("testingKey1")
	size, err := mp.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(9), "Map evict failed.")
	found, err := mp.ContainsKey("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "Map evict failed.")
}

func TestMapProxy_EvictWithNilKey(t *testing.T) {
	_, err := mp.Evict(nil)
	require.Errorf(t, err, "evict did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_EvictAll(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.EvictAll()
	size, err := mp.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "Map evict failed.")
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
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "Key should not be locked.")
	err = mp.Lock("testingKey")
	if err != nil {
		t.Fatal(err)
	}
	locked, err = mp.IsLocked("testingKey")
	require.NoError(t, err)
	assert.Equalf(t, locked, true, "Key should be locked.")
	err = mp.Unlock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "Key should not be locked.")

}

func TestMapProxy_IsLockedWithNilKey(t *testing.T) {
	_, err := mp.IsLocked(nil)
	require.Errorf(t, err, "isLocked did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_UnlockWithNilKey(t *testing.T) {
	err := mp.Unlock(nil)
	require.Errorf(t, err, "unlock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_LockWithLeaseTime(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	mp.LockWithLeaseTime("testingKey", 10*time.Millisecond)
	test.AssertTrueEventually(t, func() bool {
		locked, err := mp.IsLocked("testingKey")
		return err == nil && !locked
	})
}

func TestMapProxy_LocktWithNilKey(t *testing.T) {
	err := mp.Lock(nil)
	require.Errorf(t, err, "lock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryLock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1*time.Millisecond, 2*time.Millisecond)
	require.NoError(t, err)
	assert.Equalf(t, ok, true, "Try Lock failed")
	test.AssertTrueEventually(t, func() bool {
		locked, err := mp.IsLocked("testingKey")
		return err == nil && !locked
	})
	mp.ForceUnlock("testingKey")
	mp.Clear()
}

func TestMapProxy_ForceUnlockWithNilKey(t *testing.T) {
	err := mp.ForceUnlock(nil)
	require.Errorf(t, err, "forceUnlock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_TryLockWithNilKey(t *testing.T) {
	_, err := mp.TryLock(nil)
	require.Errorf(t, err, "tryLock did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ForceUnlock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1*time.Second, 20*time.Second)
	require.NoError(t, err)
	assert.Equalf(t, ok, true, "Try Lock failed")
	mp.ForceUnlock("testingKey")
	locked, err := mp.IsLocked("testingKey")
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "Key should not be locked.")
	mp.Unlock("testingKey")
	mp.Clear()
}

func TestMapProxy_Replace(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.Replace("testingKey1", "testingValue2")
	require.NoError(t, err)
	assert.Equalf(t, replaced, "testingValue1", "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, "testingValue2", "Map Replace failed.")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNilKey(t *testing.T) {
	_, err := mp.Replace(nil, "test")
	require.Errorf(t, err, "replace did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNilValue(t *testing.T) {
	_, err := mp.Replace("test", nil)
	require.Errorf(t, err, "replace did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_Size(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	size, err := mp.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(10), "Map size returned a wrong value")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSame(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue1", "testingValue2")
	require.NoError(t, err)
	assert.Equalf(t, replaced, true, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, "testingValue2", "Map ReplaceIfSame failed.")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilKey(t *testing.T) {
	_, err := mp.ReplaceIfSame(nil, "test", "test")
	require.Errorf(t, err, "replaceIfSame did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilOldValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", nil, "test")
	require.Errorf(t, err, "replaceIfSame did not return an error for nil oldValue")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNilNewValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", "test", nil)
	require.Errorf(t, err, "replaceIfSame did not return an error for nil newValue")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWhenDifferent(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue3", "testingValue2")
	require.NoError(t, err)
	assert.Equalf(t, replaced, false, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, "testingValue1", "Map ReplaceIfSame failed.")
	mp.Clear()
}

func TestMapProxy_Set(t *testing.T) {
	err := mp.Set("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_SetWithNilKey(t *testing.T) {
	err := mp.Set(nil, "test")
	require.Errorf(t, err, "Set did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_SetWithNilValue(t *testing.T) {
	err := mp.Set("test", nil)
	require.Errorf(t, err, "set did not return an error for nil value")
	mp.Clear()
}

func TestMapProxy_SetWithTTL(t *testing.T) {
	err := mp.SetWithTTL("testingKey1", "testingValue1", 0*time.Second)
	if err != nil {
		t.Error(err)
	}
	_, err = mp.Get("testingKey1")
	require.NoError(t, err)
	mp.SetWithTTL("testingKey1", "testingValue2", 1*time.Millisecond)
	test.AssertTrueEventually(t, func() bool {
		newValue, err := mp.Get("testingKey1")
		return err == nil && newValue == nil
	})
	mp.Clear()
}

func TestMapProxy_PutIfAbsent(t *testing.T) {
	_, err := mp.PutIfAbsent("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNilKey(t *testing.T) {
	_, err := mp.PutIfAbsent(nil, "test")
	require.Errorf(t, err, "putIfAbsent did not return an error for nil key")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNilValue(t *testing.T) {
	_, err := mp.PutIfAbsent("test", nil)
	require.Errorf(t, err, "putIfAbsent did not return an error for nil value")
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
	require.Errorf(t, err, "putAll did not return an error for nil map")
	mp.Clear()
}

func TestMapProxy_KeySet(t *testing.T) {
	var expecteds = make([]string, 10)
	var ret = make([]string, 10)
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
	keySet, _ := mp.KeySetWithPredicate(predicate.Equal("this", "5"))
	if len(keySet) != 1 || keySet[0].(string) != expected {
		t.Fatalf("map KeySetWithPredicate failed")
	}
}

func TestMapProxy_KeySetWithPredicateWithNilPredicate(t *testing.T) {
	_, err := mp.KeySetWithPredicate(nil)
	require.Errorf(t, err, "keySetWithPredicate did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_Values(t *testing.T) {
	var expecteds = make([]string, 10)
	var ret = make([]string, 10)
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
	values, _ := mp.ValuesWithPredicate(predicate.Equal("this", "5"))
	if len(values) != 1 || values[0].(string) != expected {
		t.Fatalf("map ValuesWithPredicate failed")
	}
}

func TestMapProxy_ValuesWithPredicateWithNilPredicate(t *testing.T) {
	_, err := mp.ValuesWithPredicate(nil)
	require.Errorf(t, err, "ValuesWithPredicate did not return an error for nil predicate")
	mp.Clear()
}

func TestMapProxy_EntrySetWithPredicate(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	searchedMap := make(map[interface{}]interface{})
	values := []string{"value1", "wantedValue", "wantedValue", "value2", "value3",
		"wantedValue", "wantedValue", "value4", "value5", "wantedValue"}
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
		entryList, err := mp.EntrySetWithPredicate(predicate.SQL("this == wantedValue"))
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
		for k := range testMap {
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
	require.Errorf(t, err, "GetAll did not return an error for nil keys")
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
	require.NoError(t, err)
	assert.Equalf(t, entryView.Key(), "key", "Map GetEntryView returned a wrong view.")
	require.NoError(t, err)
	assert.Equalf(t, entryView.Value(), "newValue", "Map GetEntryView returned a wrong view.")
	require.NoError(t, err)
	assert.Equalf(t, entryView.Hits(), int64(2), "Map GetEntryView returned a wrong view.")
	require.NoError(t, err)
	assert.Equalf(t, entryView.EvictionCriteriaNumber(), int64(0), "Map GetEntryView returned a wrong view.")
	require.NoError(t, err)
	assert.Equalf(t, entryView.Version(), int64(1), "Map GetEntryView returned a wrong view.")
	if cost := entryView.Cost(); cost <= 0 {
		t.Fatal("entryView cost should be greater than 0.")
	}
	if creationTime := entryView.CreationTime(); !creationTime.After(time.Time{}) {
		t.Fatal("entryView creationTime should be greater than 0.")
	}
	if expirationTime := entryView.ExpirationTime(); !expirationTime.After(time.Time{}) {
		t.Fatal("entryView expirationTime should be greater than 0.")
	}
	if lastAccessTime := entryView.LastAccessTime(); !lastAccessTime.After(time.Time{}) {
		t.Fatal("entryView lastAccessTime should be greater than 0.")
	}
	if lastUpdateTime := entryView.LastUpdateTime(); !lastUpdateTime.After(time.Time{}) {
		t.Fatal("entryView lastUpdateTime should be greater than 0.")
	}
	if ttl := entryView.TTL(); ttl <= 0 {
		t.Fatal("entryView ttl should be greater than 0.")
	}
	require.NoError(t, err)
	assert.Equalf(t, entryView.LastStoredTime(), time.Time{}, "Map GetEntryView returned a wrong view.")
	mp.Clear()
}

func TestMapProxy_GetEntryViewWithNilKey(t *testing.T) {
	_, err := mp.GetEntryView(nil)
	require.Errorf(t, err, "GetEntryView did not return an error for nil key")
	mp.Clear()
}

type entryListener struct {
	wg       *sync.WaitGroup
	event    core.EntryEvent
	mapEvent core.MapEvent
}

func (l *entryListener) EntryAdded(event core.EntryEvent) {
	l.event = event
	l.wg.Done()
}

func (l *entryListener) EntryUpdated(event core.EntryEvent) {
	l.wg.Done()
}

func (l *entryListener) EntryRemoved(event core.EntryEvent) {
	l.wg.Done()
}

func (l *entryListener) EntryEvicted(event core.EntryEvent) {
	l.wg.Done()
}

func (l *entryListener) MapEvicted(event core.MapEvent) {
	l.mapEvent = event
	l.wg.Done()
}

func (l *entryListener) MapCleared(event core.MapEvent) {
	l.wg.Done()
}

func TestMapProxy_AddEntryListener_IllegalListener(t *testing.T) {
	_, err := mp.AddEntryListener(5, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Map.AddEntryListener should return HazelcastIllegalArgumentError")
	}
}

func TestMapProxy_AddEntryListenerAdded(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, true)
	require.NoError(t, err)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Name(), "myMap", "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Key(), "key123", "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Value(), "value", "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.OldValue(), nil, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.MergingValue(), nil, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.EventType(), int32(1), "AddEntryListener entryAdded failed")

	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerAddedWithIncludeValueFalse(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, false)
	require.NoError(t, err)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Name(), "myMap", "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Key(), "key123", "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Value(), nil, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.OldValue(), nil, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.MergingValue(), nil, "AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.EventType(), int32(1), "AddEntryListener entryAdded failed")
	assert.Equal(t, entryListener.event.Member().UUID(), member.UUID)

	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerUpdated(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryAdded := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryAdded, true)
	require.NoError(t, err)
	wg.Add(2)
	mp.Put("key1", "value")
	mp.Put("key1", "value")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryUpdated failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerEvicted(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, true)
	require.NoError(t, err)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Evict("test")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryEvicted failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerRemoved(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, true)
	require.NoError(t, err)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Remove("test")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryRemoved failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerEvictAll(t *testing.T) {

	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, true)
	require.NoError(t, err)
	wg.Add(2)
	mp.Put("test", "key")
	mp.EvictAll()
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryEvictAll failed")
	assert.Equalf(t, entryListener.mapEvent.EventType(), int32(16), "AddEntryListener entryEvictAll failed")
	assert.Equalf(t, entryListener.mapEvent.Name(), "myMap", "AddEntryListener entryEvictAll failed")
	assert.NotEmpty(t, entryListener.mapEvent.String())
	assert.Equal(t, entryListener.mapEvent.Member().UUID(), member.UUID)
	assert.Equalf(t, entryListener.mapEvent.NumberOfAffectedEntries(), int32(1), "AddEntryListener entryEvictAll failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerClear(t *testing.T) {

	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListener(entryListener, true)
	require.NoError(t, err)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Clear()
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListener entryClear failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerWithPredicate_IllegalListener(t *testing.T) {
	_, err := mp.AddEntryListenerWithPredicate(5, nil, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Map.AddEntryListenerWithPredicate should return HazelcastIllegalArgumentError")
	}
}

func TestMapProxy_AddEntryListenerWithPredicate(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListenerWithPredicate(entryListener, predicate.Equal("this", "value"), true)
	require.NoError(t, err)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListenerWithPredicate failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKey_IllegalListener(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(5, nil, nil, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Map.AddEntryListenerToKey should return HazelcastIllegalArgumentError")
	}
}

func TestMapProxy_AddEntryListenerToKey(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListenerToKey(entryListener, "key1", true)
	require.NoError(t, err)
	wg.Add(1)
	mp.Put("key1", "value1")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListenerToKey failed")
	wg.Add(1)
	mp.Put("key2", "value1")
	timeout = test.WaitTimeout(wg, test.Timeout/20)
	assert.Equalf(t, true, timeout, "AddEntryListenerToKey failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithPredicate_IllegalListener(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(5, nil, nil, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Map.AddEntryListenerToKeyWithPredicate should return HazelcastIllegalArgumentError")
	}
}

func TestMapProxy_AddEntryListenerToKeyWithPredicate(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListenerToKeyWithPredicate(entryListener, predicate.Equal("this", "value1"), "key1", true)
	require.NoError(t, err)
	wg.Add(1)
	mp.Put("key1", "value1")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListenerToKeyWithPredicate failed")
	wg.Add(1)
	mp.Put("key1", "value2")
	timeout = test.WaitTimeout(wg, test.Timeout/20)
	assert.Equalf(t, true, timeout, "AddEntryListenerToKeyWithPredicate failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_RemoveEntryListenerToKeyWithInvalidRegistrationID(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := mp.AddEntryListenerToKey(entryListener, "key1", true)
	require.NoError(t, err)
	invalidRegistrationID := "invalid"
	removed, _ := mp.RemoveEntryListener(invalidRegistrationID)
	if removed {
		t.Fatal("remove entry listener to key with invalid registration id failed")
	}
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
}

func TestMapProxy_ExecuteOnKey(t *testing.T) {
	config := hazelcast.NewConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	testKey := "testingKey1"
	testValue := "testingValue"
	mp2.Put(testKey, testValue)
	value, err := mp2.ExecuteOnKey(testKey, processor)
	require.NoError(t, err)
	assert.Equalf(t, value, expectedValue, "ExecuteOnKey failed.")
	newValue, err := mp2.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, expectedValue, "ExecuteOnKey failed")
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnKeys(t *testing.T) {

	config := hazelcast.NewConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)
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
	require.NoError(t, err)
	assert.Equalf(t, len(result), 2, "ExecuteOnKeys failed.")
	newValue, err := mp2.Get("testingKey1")
	require.NoError(t, err)
	assert.Equalf(t, newValue, expectedValue, "ExecuteOnKeys failed")
	newValue, err = mp2.Get("testingKey2")
	require.NoError(t, err)
	assert.Equalf(t, newValue, expectedValue, "ExecuteOnKeys failed")
	mp2.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnEntries(t *testing.T) {
	config := hazelcast.NewConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp2.Put(testKey, testValue)
	}
	result, err := mp2.ExecuteOnEntries(processor)
	for _, pair := range result {
		require.NoError(t, err)
		assert.Equalf(t, pair.Value(), expectedValue, "ExecuteOnEntries failed")
		newValue, err := mp2.Get(pair.Key())
		require.NoError(t, err)
		assert.Equalf(t, newValue, expectedValue, "ExecuteOnEntries failed")
	}
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnEntriesWithPredicate(t *testing.T) {
	config := hazelcast.NewConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := int32(i)
		mp2.Put(testKey, testValue)
	}
	result, err := mp2.ExecuteOnEntriesWithPredicate(processor, predicate.GreaterThan("this", int32(6)))
	if len(result) != 3 {
		t.Fatal("ExecuteOnEntriesWithPredicate failed")
	}
	for _, pair := range result {
		require.NoError(t, err)
		assert.Equalf(t, pair.Value(), expectedValue, "ExecuteOnEntriesWithPredicate failed")
		newValue, err := mp2.Get(pair.Key())
		require.NoError(t, err)
		assert.Equalf(t, newValue, expectedValue, "ExecuteOnEntriesWithPredicate failed")
	}
	mp.Clear()
	client.Shutdown()
}

func TestMapProxy_ExecuteOnKeyWithNonRegisteredProcessor(t *testing.T) {
	config := hazelcast.NewConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue, 68)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)
	mp2, _ := client.GetMap("testMap2")
	testKey := "testingKey1"
	testValue := "testingValue"
	mp2.Put(testKey, testValue)
	_, err := mp2.ExecuteOnKey(testKey, processor)
	require.Errorf(t, err, "non registered processor should return an error")
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
	require.NoError(t, err)
	assert.Nilf(t, res, "get returned a wrong value")
}

type simpleEntryProcessor struct {
	classID           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor(value string, factoryID int32) *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classID: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryID: factoryID, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryID            int32
}

func (idf *identifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == idf.simpleEntryProcessor.classID {
		return &simpleEntryProcessor{classID: 1}
	}
	return nil
}

func (p *simpleEntryProcessor) ReadData(input serialization.DataInput) error {
	p.value = input.ReadUTF()
	return input.Error()
}

func (p *simpleEntryProcessor) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(p.value)
	return nil
}

func (p *simpleEntryProcessor) FactoryID() int32 {
	return p.identifiedFactory.factoryID
}

func (p *simpleEntryProcessor) ClassID() int32 {
	return p.classID
}

// Serialization error checks

type student struct {
	age int32
}

func TestMapProxy_PutWithNonSerializableKey(t *testing.T) {
	_, err := mp.Put(student{10}, "test")
	require.Errorf(t, err, "put did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutWithNonSerializableValue(t *testing.T) {
	_, err := mp.Put("test", student{10})
	require.Errorf(t, err, "put did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_TryPutWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryPut(student{10}, "test")
	require.Errorf(t, err, "tryPut did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_TryPutWithNonSerializableValue(t *testing.T) {
	_, err := mp.TryPut("test", student{10})
	require.Errorf(t, err, "tryPut did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNonSerializableKey(t *testing.T) {
	err := mp.PutTransient(student{10}, "test", 1*time.Second)
	require.Errorf(t, err, "putTransient did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutTransientWithNonSerializableValue(t *testing.T) {
	err := mp.PutTransient("test", student{10}, 1*time.Second)
	require.Errorf(t, err, "putTransient did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_GetWithNonSerializableKey(t *testing.T) {
	_, err := mp.Get(student{10})
	require.Errorf(t, err, "get did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNonSerializableKey(t *testing.T) {
	_, err := mp.RemoveIfSame(student{10}, "test")
	require.Errorf(t, err, "removeIfSame did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_RemoveIfSameWithNonSerializableValue(t *testing.T) {
	_, err := mp.RemoveIfSame("test", student{10})
	require.Errorf(t, err, "removeIfSame did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_TryRemoveWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryRemove(student{10}, 1*time.Second)
	require.Errorf(t, err, "tryRemove did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ContainsKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.ContainsKey(student{10})
	require.Errorf(t, err, "containsKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ContainsValueWithNonSerializableValue(t *testing.T) {
	_, err := mp.ContainsValue(student{})
	require.Errorf(t, err, "containsValue did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_DeleteWithNonSerializableKey(t *testing.T) {
	err := mp.Delete(student{})
	require.Errorf(t, err, "delete did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_EvictWithNonSerializableKey(t *testing.T) {
	_, err := mp.Evict(student{})
	require.Errorf(t, err, "evict did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_LockWithNonSerializableKey(t *testing.T) {
	err := mp.Lock(student{})
	require.Errorf(t, err, "lock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_TryLockWithNonSerializableKey(t *testing.T) {
	_, err := mp.TryLock(student{})
	require.Errorf(t, err, "tryLock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_UnlockWithNonSerializableKey(t *testing.T) {
	err := mp.Unlock(student{})
	require.Errorf(t, err, "unlock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ForceUnlockWithNonSerializableKey(t *testing.T) {
	err := mp.ForceUnlock(student{})
	require.Errorf(t, err, "forceUnlock did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_IsLockedWithNonSerializableKey(t *testing.T) {
	_, err := mp.IsLocked(student{})
	require.Errorf(t, err, "isLocked did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableKey(t *testing.T) {
	_, err := mp.Replace(student{}, "test")
	require.Errorf(t, err, "replace did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableValue(t *testing.T) {
	_, err := mp.Replace("test", student{})
	require.Errorf(t, err, "replace did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_ReplaceIfSameWithNonSerializableKey(t *testing.T) {
	_, err := mp.ReplaceIfSame(student{}, "test", "test")
	require.Errorf(t, err, "replaceIfSame did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableOldValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", student{}, "test")
	require.Errorf(t, err, "replaceIfSame did not return an error for nonserializable oldValue")
	mp.Clear()
}

func TestMapProxy_ReplaceWithNonSerializableNewValue(t *testing.T) {
	_, err := mp.ReplaceIfSame("test", "test", student{})
	require.Errorf(t, err, "replaceIfSame did not return an error for nonserializable newValue")
	mp.Clear()
}

func TestMapProxy_SetWithNonSerializableKey(t *testing.T) {
	err := mp.Set(student{}, "test")
	require.Errorf(t, err, "set did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_SetWithNonSerializableValue(t *testing.T) {
	err := mp.Set("test", student{})
	require.Errorf(t, err, "set did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNonSerializableKey(t *testing.T) {
	_, err := mp.PutIfAbsent(student{}, "test")
	require.Errorf(t, err, "putIfAbsent did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_PutIfAbsentWithNonSerializableValue(t *testing.T) {
	_, err := mp.PutIfAbsent("test", student{})
	require.Errorf(t, err, "putIfAbsent did not return an error for nonserializable value")
	mp.Clear()
}

func TestMapProxy_PutAllWithNonSerializableMapKey(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	testMap[student{}] = 5
	err := mp.PutAll(testMap)
	require.Errorf(t, err, "putAll did not return an error for nonserializable map key")
	mp.Clear()
}

func TestMapProxy_PutAllWithNonSerializableMapValue(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	testMap[5] = student{}
	err := mp.PutAll(testMap)
	require.Errorf(t, err, "putAll did not return an error for nonserializable map value")
	mp.Clear()
}

func TestMapProxy_GetAllWithNonSerializableKey(t *testing.T) {
	testSlice := make([]interface{}, 1)
	testSlice[0] = student{}
	_, err := mp.GetAll(testSlice)
	require.Errorf(t, err, "getAll did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_GetEntryViewWithNonSerializableKey(t *testing.T) {
	_, err := mp.GetEntryView(student{})
	require.Errorf(t, err, "getEntryView did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.AddEntryListenerToKey(nil, student{}, false)
	require.Errorf(t, err, "addEntryListenerToKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_AddEntryListenerToKeyWithPredicateWithNonSerializableKey(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(nil, nil, student{}, false)
	require.Errorf(t, err, "addEntryListenerToKeyWithPredicate did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeyWithNonSerializableKey(t *testing.T) {
	_, err := mp.ExecuteOnKey(student{}, nil)
	require.Errorf(t, err, "executeOnKey did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnEntriesWithNonSerializableKey(t *testing.T) {
	_, err := mp.ExecuteOnEntries(student{})
	require.Errorf(t, err, "executeOnEntries did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeysWithNonSerializableKey(t *testing.T) {
	testSlice := make([]interface{}, 1)
	testSlice[0] = student{}
	_, err := mp.ExecuteOnKeys(testSlice, nil)
	require.Errorf(t, err, "executeOnKeys did not return an error for nonserializable key")
	mp.Clear()
}

func TestMapProxy_ExecuteOnKeysWithNonSerializableProcessor(t *testing.T) {

	_, err := mp.ExecuteOnKeys(nil, student{})
	require.Errorf(t, err, "executeOnKeys did not return an error for nonserializable processor")
	mp.Clear()
}
