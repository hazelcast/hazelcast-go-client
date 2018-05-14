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

package replicated_map

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
	"github.com/hazelcast/hazelcast-go-client/core/predicates"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/tests"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

var rmp core.ReplicatedMap
var client hazelcast.Instance

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", tests.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	rmp, _ = client.GetReplicatedMap("myReplicatedMap")
	m.Run()
	rmp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestReplicatedMapProxy_Destroy(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	rmp.Destroy()
	rmp, _ := client.GetReplicatedMap("myReplicatedMap")
	res, err := rmp.Get(testKey)
	assert.Nilf(t, err, res, "replicatedMap Destroy() failed")
}

func TestReplicatedMapProxy_Put(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	newValue := "newValue"
	_, err := rmp.Put(testKey, testValue)
	assert.ErrorNil(t, err)
	res, err := rmp.Get(testKey)
	assert.Equalf(t, err, res, testValue, "replicatedMap Put() failed")
	oldValue, err := rmp.Put(testKey, newValue)
	assert.Equalf(t, err, oldValue, testValue, "replicatedMap Put()  failed")
}

func TestReplicatedMapProxy_PutWithNilKey(t *testing.T) {
	defer rmp.Clear()
	testValue := "testingValue"
	_, err := rmp.Put(nil, testValue)
	assert.ErrorNotNil(t, err, "replicatedMap Put() failed")
}

func TestReplicatedMapProxy_PutWithNilValue(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	_, err := rmp.Put(testKey, nil)
	assert.ErrorNotNil(t, err, "replicatedMap Put() failed")
}

func TestReplicatedMapProxy_PutWithTTL(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	oldValue, err := rmp.PutWithTTL(testKey, "nextValue", 100*time.Second)
	assert.Equalf(t, err, oldValue, testValue, "replicatedMap PutWithTTL()  failed")
	res, err := rmp.Get(testKey)
	assert.Equalf(t, err, res, "nextValue", "replicatedMap PutWithTTL() failed")
}

func TestReplicatedMapProxy_PutWithTTLWithNilKey(t *testing.T) {
	defer rmp.Clear()
	testValue := "testingValue"
	_, err := rmp.PutWithTTL(nil, testValue, 100*time.Second)
	assert.ErrorNotNil(t, err, "replicatedMap PutWithTTL() failed")
}

func TestReplicatedMapProxy_PutWithTTLWithNilValue(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	_, err := rmp.PutWithTTL(testKey, nil, 100*time.Second)
	assert.ErrorNotNil(t, err, "replicatedMap PutWithTTL() failed")
}

func TestMapProxy_PutWithTTLWhenExpire(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	rmp.Put(testKey, testValue)
	rmp.PutWithTTL(testKey, "nextValue", 1*time.Millisecond)
	time.Sleep(2 * time.Second)
	res, err := rmp.Get(testKey)
	assert.Nilf(t, err, res, "replicatedMap PutWithTTL() failed")
}

func TestReplicatedMapProxy_PutAll(t *testing.T) {
	defer rmp.Clear()
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := rmp.PutAll(testMap)
	assert.ErrorNil(t, err)
	entryList, err := rmp.EntrySet()
	assert.MapEqualPairSlice(t, err, testMap, entryList, "replicatedMap PutAll() failed")
}

func TestReplicatedMapProxy_PutAllWithNilEntries(t *testing.T) {
	defer rmp.Clear()
	err := rmp.PutAll(nil)
	assert.ErrorNotNil(t, err, "replicatedMap PutAll() failed")
}

func TestReplicatedMapProxy_Get(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	_, err := rmp.Put(testKey, testValue)
	assert.ErrorNil(t, err)
	res, err := rmp.Get(testKey)
	assert.Equalf(t, err, res, testValue, "replicatedMap Get() failed")
}

func TestReplicatedMapProxy_GetWithNilKey(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	_, _ = rmp.Put(testKey, testValue)
	_, err := rmp.Get(nil)
	assert.ErrorNotNil(t, err, "replicatedMap Get() failed")
}

func TestReplicatedMapProxy_GetWithNonExistKey(t *testing.T) {
	defer rmp.Clear()
	testKey := "testingKey"
	testValue := "testingValue"
	_, err := rmp.Put("key", testValue)
	assert.ErrorNil(t, err)
	res, err := rmp.Get(testKey)
	assert.Nilf(t, err, res, "replicatedMap Get() failed")
}

func TestReplicatedMapProxy_ContainsKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsKey("testKey5")
	assert.Equalf(t, err, res, true, "replicatedMap ContainsKey() failed")
}

func TestReplicatedMapProxy_ContainsKeyWithNilKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	_, err := rmp.ContainsKey(nil)
	assert.ErrorNotNil(t, err, "replicatedMap ContainsKey() failed")
}

func TestReplicatedMapProxy_ContainsKeyWithNonExistKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsKey("testKey11")
	assert.Equalf(t, err, res, false, "replicatedMap ContainsKey() failed")
}

func TestReplicatedMapProxy_ContainsValue(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsValue("testValue5")
	assert.Equalf(t, err, res, true, "replicatedMap ContainsValue() failed")
}

func TestReplicatedMapProxy_ContainsValueWithNilValue(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	_, err := rmp.ContainsValue(nil)
	assert.ErrorNotNil(t, err, "replicatedMap ContainsValue() failed")
}

func TestReplicatedMapProxy_ContainsWithNonExistValue(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	res, err := rmp.ContainsValue("testValue11")
	assert.Equalf(t, err, res, false, "replicatedMap ContainsValue() failed")
}

func TestReplicatedMapProxy_Clear(t *testing.T) {
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	err := rmp.Clear()
	size, _ := rmp.Size()
	assert.Equalf(t, err, size, int32(0), "replicatedMap Clear() failed")
}

func TestReplicatedMapProxy_Remove(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	value, err := rmp.Remove("testKey5")
	assert.Equalf(t, err, value, "testValue5", "replicatedMap Remove() failed")
	size, _ := rmp.Size()
	assert.Equalf(t, err, size, int32(9), "replicatedMap Remove() failed")
}

func TestReplicatedMapProxy_RemoveWithNilKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	_, err := rmp.Remove(nil)
	assert.ErrorNotNil(t, err, "replicatedMap Remove() failed")
}

func TestReplicatedMapProxy_RemoveWithNonExistKey(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	value, err := rmp.Remove("testKey11")
	assert.Nilf(t, err, value, "replicatedMap Remove() failed")
}

func TestReplicatedMapProxy_IsEmpty(t *testing.T) {
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	empty, err := rmp.IsEmpty()
	assert.Equalf(t, err, empty, false, "replicatedMap IsEmpty() failed")
	rmp.Clear()
	empty, err = rmp.IsEmpty()
	assert.Equalf(t, err, empty, true, "replicatedMap IsEmpty() failed")
}

func TestReplicatedMapProxy_Size(t *testing.T) {
	defer rmp.Clear()
	for i := 1; i <= 10; i++ {
		rmp.Put("testKey"+strconv.Itoa(i), "testValue"+strconv.Itoa(i))
	}
	size, err := rmp.Size()
	assert.Equalf(t, err, size, int32(10), "replicatedMap Size() failed")
}

func TestReplicatedMapProxy_Values(t *testing.T) {
	defer rmp.Clear()
	testMap := make(map[interface{}]interface{})
	expected := make([]interface{}, 0)
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
		expected = append(expected, "testingValue"+strconv.Itoa(i))
	}
	rmp.PutAll(testMap)
	values, err := rmp.Values()
	assert.SlicesHaveSameElements(t, err, expected, values, "replicatedMap Values() failed")
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
		t.Fatalf("replicatedMap KeySet() failed")
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
	assert.MapEqualPairSlice(t, err, testMap, entrySet, "replicatedMap EntrySet() failed")
}

func TestReplicatedMapProxy_AddEntryListener_IllegalListener(t *testing.T) {
	_, err := rmp.AddEntryListener(5)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("ReplicatedMap.AddEntryListener should return HazelcastIllegalArgumentError")
	}
}

func TestReplicatedMapProxy_AddEntryListener(t *testing.T) {
	defer rmp.Clear()
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := rmp.AddEntryListener(entryListener)
	defer rmp.RemoveEntryListener(registrationID)
	assert.Equal(t, err, nil, nil)
	wg.Add(1)
	rmp.Put("key123", "value")
	timeout := tests.WaitTimeout(wg, tests.Timeout)
	assert.Equalf(t, nil, false, timeout, "replicatedMap AddEntryListener entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.Key(), "key123", "replicatedMap AddEntryListener entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.Value(), "value", "replicatedMap AddEntryListener entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.OldValue(), nil, "replicatedMap AddEntryListener entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.MergingValue(), nil, "replicatedMap AddEntryListener entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.EventType(), int32(1), "replicatedMap AddEntryListener entryAdded failed")
}

func TestReplicatedMapProxy_AddEntryListenerWithPredicate_IllegalListener(t *testing.T) {
	_, err := rmp.AddEntryListenerWithPredicate(5, nil)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("ReplicatedMap.AddEntryListenerWithPredicate should return HazelcastIllegalArgumentError")
	}
}

func TestReplicatedMapProxy_AddEntryListenerWithPredicate(t *testing.T) {
	defer rmp.Clear()
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := rmp.AddEntryListenerWithPredicate(entryListener, predicates.Equal("this", "value"))
	defer rmp.RemoveEntryListener(registrationID)
	assert.Equal(t, err, nil, nil)
	wg.Add(1)
	rmp.Put("key123", "value")
	timeout := tests.WaitTimeout(wg, tests.Timeout)
	assert.Equalf(t, nil, false, timeout, "replicatedMap AddEntryListenerWithPredicate failed")
	assert.Equalf(t, nil, entryListener.event.Key(), "key123", "replicatedMap AddEntryListenerWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.Value(), "value", "replicatedMap AddEntryListenerWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.OldValue(), nil, "replicatedMap AddEntryListenerWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.MergingValue(), nil, "replicatedMap AddEntryListenerWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.EventType(), int32(1), "replicatedMap AddEntryListenerWithPredicate entryAdded failed")
}

func TestReplicatedMapProxy_AddEntryListenerToKey_IllegalListener(t *testing.T) {
	_, err := rmp.AddEntryListenerToKey(5, nil)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("ReplicatedMap.AddEntryListenerToKey should return HazelcastIllegalArgumentError")
	}
}

func TestReplicatedMapProxy_AddEntryListenerToKey(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := rmp.AddEntryListenerToKey(entryListener, "key1")
	assert.Equal(t, err, nil, nil)
	wg.Add(1)
	rmp.Put("key1", "value")
	timeout := tests.WaitTimeout(wg, tests.Timeout)
	assert.Equalf(t, nil, false, timeout, "replicatedMap AddEntryListenerToKey failed")
	assert.Equalf(t, nil, entryListener.event.Key(), "key1", "replicatedMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.Value(), "value", "replicatedMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.OldValue(), nil, "replicatedMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.MergingValue(), nil, "replicatedMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.EventType(), int32(1), "replicatedMap AddEntryListenerToKey entryAdded failed")
	wg.Add(1)
	rmp.Put("key2", "value1")
	timeout = tests.WaitTimeout(wg, tests.Timeout/20)
	assert.Equalf(t, nil, true, timeout, "replicatedMap AddEntryListenerToKey failed")
	rmp.RemoveEntryListener(registrationID)
	rmp.Clear()
}

func TestReplicatedMapProxy_AddEntryListenerToKeyWithPredicate_IllegalListener(t *testing.T) {
	_, err := rmp.AddEntryListenerToKeyWithPredicate(5, nil, nil)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("ReplicatedMap.AddEntryListenerToKeyWithPredicate should return HazelcastIllegalArgumentError")
	}
}

func TestReplicatedMapProxy_AddEntryListenerToKeyWithPredicate(t *testing.T) {
	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}
	registrationID, err := rmp.AddEntryListenerToKeyWithPredicate(entryListener, predicates.Equal("this", "value1"), "key1")
	assert.Equal(t, err, nil, nil)
	wg.Add(1)
	rmp.Put("key1", "value1")
	timeout := tests.WaitTimeout(wg, tests.Timeout)
	assert.Equalf(t, nil, false, timeout, "replicatedMap AddEntryListenerToKeyWithPredicate failed")
	assert.Equalf(t, nil, entryListener.event.Key(), "key1", "replicatedMap AddEntryListenerToKeyWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.Value(), "value1",
		"replicatedMap AddEntryListenerToKeyWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.OldValue(), nil, "replicatedMap AddEntryListenerToKeyWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.MergingValue(), nil,
		"replicatedMap AddEntryListenerToKeyWithPredicate entryAdded failed")
	assert.Equalf(t, nil, entryListener.event.EventType(), int32(1),
		"replicatedMap AddEntryListenerToKeyWithPredicate entryAdded failed")

	wg.Add(1)
	rmp.Put("key1", "value2")
	timeout = tests.WaitTimeout(wg, tests.Timeout/20)
	assert.Equalf(t, nil, true, timeout, "replicatedMap AddEntryListenerToKeyWithPredicate failed")
	rmp.RemoveEntryListener(registrationID)
	rmp.Clear()
}

type entryListener struct {
	wg    *sync.WaitGroup
	event core.EntryEvent
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

func (l *entryListener) MapCleared(event core.MapEvent) {
	l.wg.Done()
}
