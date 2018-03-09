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

package multi_map

import (
	"github.com/hazelcast/hazelcast-go-client"
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

var multiMap MultiMap
var client hazelcast.IHazelcastInstance
var testKey = "testKey"
var testValue = "testValue"
var testValue2 = "testValue2"

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	multiMap, _ = client.GetMultiMap("myMultiMap")
	m.Run()
	multiMap.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestMultiMapProxy_Put(t *testing.T) {
	defer multiMap.Clear()
	added, err := multiMap.Put(testKey, testValue)
	AssertEqualf(t, err, added, true, "multiMap put() failed ")
	values, err := multiMap.Get(testKey)
	AssertEqualf(t, err, len(values), 1, "multiMap put() failed ")
	AssertEqualf(t, err, values[0], testValue, "multiMap put() failed ")
}

func TestMultiMapProxy_PutWithNil(t *testing.T) {
	defer multiMap.Clear()
	_, err := multiMap.Put(nil, testValue)
	AssertErrorNotNil(t, err, "multiMap put() failed ")
}

func TestMultiMapProxy_Remove(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	removed, err := multiMap.Remove(testKey, testValue)
	AssertEqualf(t, err, removed, true, "multiMap Remove() returned a wrong value")
	size, err := multiMap.Size()
	AssertEqualf(t, err, size, int32(0), "multiMap Size() should be 0.")
	found, err := multiMap.ContainsKey(testKey)
	AssertEqualf(t, err, found, false, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_RemoveWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.Remove(nil, testValue)
	AssertErrorNotNil(t, err, "multiMap Remove() failed ")
}

func TestMultiMapProxy_RemoveWithNonExist(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	removed, err := multiMap.Remove("key", testValue)
	AssertEqualf(t, err, removed, false, "multiMap Remove() returned a wrong value")
}

func TestMultiMapProxy_RemoveAll(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	removed, err := multiMap.RemoveAll(testKey)
	AssertEqualf(t, err, len(removed), 2, "multiMap RemoveAll() returned a wrong result")
	found, err := multiMap.ContainsValue(testValue)
	AssertEqualf(t, err, found, false, "multiMap RemoveAll() returned a wrong result")
	found, err = multiMap.ContainsValue(testValue2)
	AssertEqualf(t, err, found, false, "multiMap RemoveAll() returned a wrong result")
}

func TestMultiMapProxy_RemoveAllWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	_, err := multiMap.RemoveAll(nil)
	AssertErrorNotNil(t, err, "multiMap RemoveAll() failed ")
}

func TestMultiMapProxy_RemoveAllWithNonExist(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	removed, err := multiMap.RemoveAll("key")
	AssertEqualf(t, err, len(removed), 0, "multiMap RemoveAll() returned a wrong result")
}

func TestMultiMapProxy_ContainsKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsKey(testKey)
	AssertEqualf(t, err, found, true, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_ContainsKeyWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsKey(nil)
	AssertErrorNotNil(t, err, "multiMap ContainsKey() failed ")
}

func TestMultiMapProxy_ContainsKeyWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsKey("key")
	AssertEqualf(t, err, found, false, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_ContainsValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsValue(testValue)
	AssertEqualf(t, err, found, true, "multiMap ContainsValue() returned a wrong result")
}

func TestMultiMapProxy_ContainsValueWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsValue(nil)
	AssertErrorNotNil(t, err, "multiMap ContainsValue() failed ")
}

func TestMultiMapProxy_ContainsValueWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsValue("value")
	AssertEqualf(t, err, found, false, "multiMap ContainsValue() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntry(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry(testKey, testValue)
	AssertEqualf(t, err, found, true, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntryWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsEntry(nil, testValue)
	AssertErrorNotNil(t, err, "multiMap ContainsEntry() failed ")
}

func TestMultiMapProxy_ContainsEntryWithNilValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsEntry(testKey, nil)
	AssertErrorNotNil(t, err, "multiMap ContainsEntry() failed ")
}

func TestMultiMapProxy_ContainsEntryWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry("key", testValue)
	AssertEqualf(t, err, found, false, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntryWithNonExistValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry(testKey, "value")
	AssertEqualf(t, err, found, false, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_Clear(t *testing.T) {
	multiMap.Put(testKey, testValue)
	err := multiMap.Clear()
	if err != nil {
		t.Fatal(err)
	} else {
		size, err := multiMap.Size()
		AssertEqualf(t, err, size, int32(0), "multiMap Clear() failed")
	}
}

func TestMultiMapProxy_Size(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey+strconv.Itoa(i), testValue+strconv.Itoa(i))
	}
	size, err := multiMap.Size()
	AssertEqualf(t, err, size, int32(10), "multiMap Size() returned a wrong value")
}

func TestMultiMapProxy_ValueCount(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey, testValue+strconv.Itoa(i))
	}
	size, err := multiMap.ValueCount(testKey)
	AssertEqualf(t, err, size, int32(10), "multiMap ValueCount() returned a wrong value")
}

func TestMultiMapProxy_ValueCountWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey, testValue+strconv.Itoa(i))
	}
	_, err := multiMap.ValueCount(nil)
	AssertErrorNotNil(t, err, "multiMap ValueCount() failed ")
}

func TestMultiMapProxy_Values(t *testing.T) {
	defer multiMap.Clear()
	var expecteds = make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		multiMap.Put(strconv.Itoa(i), strconv.Itoa(i))
		expecteds[i] = strconv.Itoa(i)
	}
	values, err := multiMap.Values()
	AssertSlicesHaveSameElements(t, err, values, expecteds, "multiMap Values() failed")
}

func TestMultiMapProxy_KeySet(t *testing.T) {
	var expecteds = make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		multiMap.Put(strconv.Itoa(i), int32(i))
		expecteds[i] = strconv.Itoa(i)
	}
	keySlice, err := multiMap.KeySet()
	AssertSlicesHaveSameElements(t, err, keySlice, expecteds, "multiMap KeySet() failed")
}

func TestMultiMapProxy_EntrySet(t *testing.T) {
	defer multiMap.Clear()
	var expectedKeys = make([]interface{}, 10)
	var expectedValues = make([]interface{}, 10)
	var resultKeys = make([]interface{}, 10)
	var resultValues = make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		multiMap.Put(strconv.Itoa(i), int32(i))
		expectedKeys[i] = strconv.Itoa(i)
		expectedValues[i] = int32(i)
	}
	pairSlice, err := multiMap.EntrySet()
	for index, pair := range pairSlice {
		resultKeys[index] = pair.Key()
		resultValues[index] = pair.Value()
	}
	AssertSlicesHaveSameElements(t, err, resultKeys, expectedKeys, "multiMap EntrySet() failed")
	AssertSlicesHaveSameElements(t, err, resultValues, expectedValues, "multiMap EntrySet() failed")
}

func TestMultiMapProxy_AddEntryListenerAdded(t *testing.T) {
	defer multiMap.Clear()
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryListener := &EntryListener{wg: wg}
	registrationId, err := multiMap.AddEntryListener(entryListener, true)
	defer multiMap.RemoveEntryListener(registrationId)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	multiMap.Put(testKey, testValue)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "multiMap AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.Key(), testKey, "multiMap AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.Value(), testValue, "multiMap AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.OldValue(), nil, "multiMap AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.MergingValue(), nil, "multiMap AddEntryListener entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.EventType(), int32(1), "multiMap AddEntryListener entryAdded failed")

}

func TestMultiMapProxy_EntryListenerToKey(t *testing.T) {
	defer multiMap.Clear()
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryListener := &EntryListener{wg: wg}
	registrationId, err := multiMap.AddEntryListenerToKey(entryListener, "key1", true)
	defer multiMap.RemoveEntryListener(registrationId)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	multiMap.Put("key1", "value1")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListenerToKey failed")
	AssertEqualf(t, nil, entryListener.event.Key(), "key1", "multiMap AddEntryListenerToKey entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.Value(), "value1", "multiMap AddEntryListenerToKey entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.OldValue(), nil, "multiMap AddEntryListenerToKey entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.MergingValue(), nil, "multiMap AddEntryListenerToKey entryAdded failed")
	AssertEqualf(t, nil, entryListener.event.EventType(), int32(1), "multiMap AddEntryListenerToKey entryAdded failed")
	wg.Add(1)
	multiMap.Put("key2", "value1")
	timeout = WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "multiMap AddEntryListenerToKey failed")
}

func TestMultiMapProxy_LockWithLeaseTime(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.LockWithLeaseTime(testKey, 10, time.Millisecond)
	time.Sleep(5 * time.Second)
	locked, err := multiMap.IsLocked(testKey)
	AssertEqualf(t, err, locked, false, "multiMap LockWithLeaseTime() failed.")
}

func TestMultiMapProxy_LockWithLeaseTimeWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	err := multiMap.LockWithLeaseTime(nil, 10, time.Millisecond)
	AssertErrorNotNil(t, err, "multiMap LockWithLeaseTime() failed ")
}

func TestMultiMapProxy_Lock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.LockWithLeaseTime(testKey, 10, time.Millisecond)
	time.Sleep(5 * time.Second)
	locked, err := multiMap.IsLocked(testKey)
	AssertEqualf(t, err, locked, false, "multiMap Lock() failed.")
}

func TestMultiMapProxy_IsLocked(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	locked, err := multiMap.IsLocked(testKey)
	AssertEqualf(t, err, locked, false, "multiMap IsLock() failed.")
	err = multiMap.Lock(testKey)
	if err != nil {
		t.Fatal(err)
	}
	locked, err = multiMap.IsLocked(testKey)
	AssertEqualf(t, err, locked, true, "multiMap Lock() failed")
	err = multiMap.Unlock(testKey)
	if err != nil {
		t.Error(err)
	}
	locked, err = multiMap.IsLocked(testKey)
	AssertEqualf(t, err, locked, false, "multiMap Lock() failed")
}

func TestMultiMapProxy_IsLockedWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.IsLocked(nil)
	AssertErrorNotNil(t, err, "multiMap IsLocked() failed")
}

func TestMultiMapProxy_TryLock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	ok, err := multiMap.TryLockWithTimeoutAndLease(testKey, 1, time.Second, 2, time.Second)
	AssertEqualf(t, err, ok, true, "multiMap TryLock() failed.")
	time.Sleep(5 * time.Second)
	locked, err := multiMap.IsLocked(testKey)
	defer multiMap.ForceUnlock(testKey)
	AssertEqualf(t, err, locked, false, "multiMap TryLock() failed.")
}

func TestMultiMapProxy_TryLockWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.TryLockWithTimeoutAndLease(nil, 1, time.Second, 2, time.Second)
	AssertErrorNotNil(t, err, "multiMap TryLock() failed")
}

func TestMultiMapProxy_ForceUnlock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	ok, err := multiMap.TryLockWithTimeoutAndLease(testKey, 1, time.Second, 20, time.Second)
	AssertEqualf(t, err, ok, true, "multiMap ForceUnLock() failed.")
	multiMap.ForceUnlock(testKey)
	locked, err := multiMap.IsLocked(testKey)
	defer multiMap.Unlock(testKey)
	AssertEqualf(t, err, locked, false, "multiMap ForceUnLock() failed.")
}

func TestMultiMapProxy_ForceUnlockWithNil(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	err := multiMap.ForceUnlock(nil)
	AssertErrorNotNil(t, err, "multiMap ForceUnlock() failed")

}

type EntryListener struct {
	wg       *sync.WaitGroup
	event    IEntryEvent
	mapEvent IMapEvent
}

func (entryListener *EntryListener) EntryAdded(event IEntryEvent) {
	entryListener.event = event
	entryListener.wg.Done()
}

func (entryListener *EntryListener) EntryUpdated(event IEntryEvent) {
	entryListener.wg.Done()
}

func (entryListener *EntryListener) EntryRemoved(event IEntryEvent) {
	entryListener.wg.Done()
}

func (entryListener *EntryListener) EntryEvicted(event IEntryEvent) {
	entryListener.wg.Done()
}

func (entryListener *EntryListener) EntryEvictAll(event IMapEvent) {
	entryListener.mapEvent = event
	entryListener.wg.Done()
}

func (entryListener *EntryListener) EntryClearAll(event IMapEvent) {
	entryListener.wg.Done()
}
