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

package proxy

import (
	"testing"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"strconv"
	"sort"
	"time"
	"reflect"
)

func TestReplicatedMapProxy_Name(t *testing.T) {
	name := "myReplicatedMap"
	AssertEqualf(t, nil, name, rmp.Name(), "Name() failed")
}

func TestReplicatedMapProxy_ServiceName(t *testing.T) {
	serviceName := common.SERVICE_NAME_REPLICATED_MAP
	AssertEqualf(t, nil, serviceName, rmp.ServiceName(), "ServiceName() failed")
}

func TestReplicatedMapProxy_PartitionKey(t *testing.T) {
	name := "myReplicatedMap"
	AssertEqualf(t, nil, name, rmp.PartitionKey(), "PartitionKey() failed")
}

func TestReplicatedMapProxy_Destroy(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	rmp.Destroy()
	rmp, _ := client.GetReplicatedMap("myReplicatedMap")
	res, err := rmp.Get(testKey)
	AssertNilf(t, err, res, "Destroy() failed")
}

func TestReplicatedMapProxy_Put(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	newValue := "newValue"
	_, err := rmp.Put(testKey, testValue)
	res, err := rmp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "Put() failed")
	oldValue, err := rmp.Put(testKey, newValue)
	AssertEqualf(t, err, oldValue, testValue, "Put()  failed")
}

func TestReplicatedMapProxy_PutWithTtl(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	oldValue, err := rmp.PutWithTtl(testKey, "nextValue", 100, time.Second)
	AssertEqualf(t, err, oldValue, testValue, "PutWithTtl()  failed")
	res, err := rmp.Get(testKey)
	AssertEqualf(t, err, res, "nextValue", "PutWithTtl() failed")
}

func TestMapProxy_PutWithTtlWhenExpire(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	rmp.PutWithTtl(testKey, "nextValue", 1, time.Millisecond)
	time.Sleep(2 * time.Second)
	res, err := rmp.Get(testKey)
	AssertNilf(t, err, res, "PutWithTtl() failed")
	mp.Clear()

}

func TestReplicatedMapProxy_PutAll(t *testing.T) {
	defer rmp.Clear()
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := rmp.PutAll(testMap)
	AssertErrorNil(t, err)
	entryList, err := rmp.EntrySet()
	AssertErrorNil(t, err)
	for _, pair := range entryList {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := testMap[key]
		if !found || expectedValue != value {
			t.Errorf("PutAll() failed")
		}
	}
}

func TestReplicatedMapProxy_Get(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	_, err := rmp.Put(testKey, testValue)
	res, err := rmp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "Get() failed")
}

func TestReplicatedMapProxy_ContainsKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsKey("testKey5")
	AssertEqualf(t, err, res, true, "ContainsKey() failed")
}

func TestReplicatedMapProxy_ContainsValue(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsValue("testValue5")
	AssertEqualf(t, err, res, true, "ContainsValue() failed")
}

func TestReplicatedMapProxy_Clear(t *testing.T) {
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	err := rmp.Clear()
	size, _ := rmp.Size()
	AssertEqualf(t, err, size, int32(0), "Clear() failed")
}

func TestReplicatedMapProxy_Remove(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	value, err := rmp.Remove("testKey5")
	AssertEqualf(t, err, value, "testValue5", "Remove() failed")
	size, _ := rmp.Size()
	AssertEqualf(t, err, size, int32(9), "Remove() failed")
}

func TestReplicatedMapProxy_IsEmpty(t *testing.T) {
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	empty, err := rmp.IsEmpty()
	AssertEqualf(t, err, empty, false, "IsEmpty() failed")
	rmp.Clear()
	empty, err = rmp.IsEmpty()
	AssertEqualf(t, err, empty, true, "IsEmpty() failed")
}

func TestReplicatedMapProxy_Size(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	size, err := rmp.Size()
	AssertEqualf(t, err, size, int32(10), "Size() failed")
}

func TestReplicatedMapProxy_Values(t *testing.T) {
	defer rmp.Clear()
	var expecteds = make([]string, 10)
	var ret = make([]string, 10)
	for i := 0; i < 10; i++ {
		rmp.Put(strconv.Itoa(i), strconv.Itoa(i))
		expecteds[i] = strconv.Itoa(i)
	}
	values, err := rmp.Values()
	for j := 0; j < 10; j++ {
		ret[j] = values[j].(string)
	}
	sort.Strings(ret)
	AssertEqualf(t, err, ret, expecteds, "Values() failed")
}

func TestReplicatedMapProxy_KeySet(t *testing.T) {
	defer rmp.Clear()
	var expecteds = make([]string, 10)
	var ret = make([]string, 10)
	for i := 0; i < 10; i++ {
		rmp.Put(strconv.Itoa(i), int32(i))
		expecteds[i] = strconv.Itoa(i)
	}
	keySet, _ := rmp.KeySet()
	for j := 0; j < 10; j++ {
		ret[j] = keySet[j].(string)
	}
	sort.Strings(ret)
	if len(keySet) != len(expecteds) || !reflect.DeepEqual(ret, expecteds) {
		t.Fatalf("KeySet() failed")
	}
}

func TestReplicatedMapProxy_EntrySet(t *testing.T) {
	defer rmp.Clear()
	testMap := make(map[interface{}]interface{})
	for i := 1; i <= 10; i++ {
		key := "testKey" + strconv.Itoa(i)
		value := "testValue" + strconv.Itoa(i)
		rmp.Put(key, value)
		testMap[key] = value
	}
	entrySet, err := rmp.EntrySet()
	AssertErrorNil(t, err)
	if len(testMap) != len(entrySet) {
		t.Fatalf("EntrySet() failed")
	}
	for _, pair := range entrySet {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := testMap[key]
		if !found || expectedValue != value {
			t.Fatalf("EntrySet() failed")
		}
	}
}

func TestReplicatedMapProxy_AddEntryListener(t *testing.T) {

}

func TestReplicatedMapProxy_AddEntryListenerWithPredicate(t *testing.T) {

}

func TestReplicatedMapProxy_AddEntryListenerToKey(t *testing.T) {

}

func TestReplicatedMapProxy_AddEntryListenerToKeyWithPredicate(t *testing.T) {

}

func TestReplicatedMapProxy_RemoveEntryListener(t *testing.T) {

}
