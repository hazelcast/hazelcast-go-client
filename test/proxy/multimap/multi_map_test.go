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

package multimap

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var multiMap core.MultiMap
var client hazelcast.Client
var testKey = "testKey"
var testValue = "testValue"
var testValue2 = "testValue2"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
	multiMap, _ = client.GetMultiMap("myMultiMap")
	m.Run()
	multiMap.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestMultiMapProxy_Put(t *testing.T) {
	defer multiMap.Clear()
	added, err := multiMap.Put(testKey, testValue)
	require.NoError(t, err)
	assert.Equalf(t, added, true, "multiMap put() failed ")
	values, err := multiMap.Get(testKey)
	require.NoError(t, err)
	assert.Equalf(t, len(values), 1, "multiMap put() failed ")
	require.NoError(t, err)
	assert.Equalf(t, values[0], testValue, "multiMap put() failed ")
}

func TestMultiMapProxy_PutWithNil(t *testing.T) {
	defer multiMap.Clear()
	_, err := multiMap.Put(nil, testValue)
	require.Errorf(t, err, "multiMap put() failed ")
}

func TestMultiMapProxy_Remove(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	removed, err := multiMap.Remove(testKey, testValue)
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "multiMap Remove() returned a wrong value")
	size, err := multiMap.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "multiMap Size() should be 0.")
	found, err := multiMap.ContainsKey(testKey)
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_RemoveWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.Remove(nil, testValue)
	require.Errorf(t, err, "multiMap Remove() failed ")
}

func TestMultiMapProxy_RemoveWithNonExist(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	removed, err := multiMap.Remove("key", testValue)
	require.NoError(t, err)
	assert.Equalf(t, removed, false, "multiMap Remove() returned a wrong value")
}

func TestMultiMapProxy_RemoveAll(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	removed, err := multiMap.RemoveAll(testKey)
	require.NoError(t, err)
	assert.Equalf(t, len(removed), 2, "multiMap RemoveAll() returned a wrong result")
	found, err := multiMap.ContainsValue(testValue)
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap RemoveAll() returned a wrong result")
	found, err = multiMap.ContainsValue(testValue2)
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap RemoveAll() returned a wrong result")
}

func TestMultiMapProxy_RemoveAllWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	_, err := multiMap.RemoveAll(nil)
	require.Errorf(t, err, "multiMap RemoveAll() failed ")
}

func TestMultiMapProxy_RemoveAllWithNonExist(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.Put(testKey, testValue2)
	removed, err := multiMap.RemoveAll("key")
	require.NoError(t, err)
	assert.Equalf(t, len(removed), 0, "multiMap RemoveAll() returned a wrong result")
}

func TestMultiMapProxy_ContainsKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsKey(testKey)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_ContainsKeyWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsKey(nil)
	require.Errorf(t, err, "multiMap ContainsKey() failed ")
}

func TestMultiMapProxy_ContainsKeyWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsKey("key")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap ContainsKey() returned a wrong result")
}

func TestMultiMapProxy_ContainsValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsValue(testValue)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "multiMap ContainsValue() returned a wrong result")
}

func TestMultiMapProxy_ContainsValueWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsValue(nil)
	require.Errorf(t, err, "multiMap ContainsValue() failed ")
}

func TestMultiMapProxy_ContainsValueWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsValue("value")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap ContainsValue() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntry(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry(testKey, testValue)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntryWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsEntry(nil, testValue)
	require.Errorf(t, err, "multiMap ContainsEntry() failed ")
}

func TestMultiMapProxy_ContainsEntryWithNilValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.ContainsEntry(testKey, nil)
	require.Errorf(t, err, "multiMap ContainsEntry() failed ")
}

func TestMultiMapProxy_ContainsEntryWithNonExistKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry("key", testValue)
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_ContainsEntryWithNonExistValue(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	found, err := multiMap.ContainsEntry(testKey, "value")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "multiMap ContainsEntry() returned a wrong result")
}

func TestMultiMapProxy_Delete(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put("testKey", "testValue1")
	multiMap.Put("testKey", "testValue2")
	multiMap.Delete("testKey")
	size, err := multiMap.Size()
	require.NoError(t, err)
	assert.Equal(t, size, int32(0))
}

func TestMultiMapProxy_Clear(t *testing.T) {
	multiMap.Put(testKey, testValue)
	err := multiMap.Clear()
	if err != nil {
		t.Fatal(err)
	} else {
		size, err := multiMap.Size()
		require.NoError(t, err)
		assert.Equalf(t, size, int32(0), "multiMap Clear() failed")
	}
}

func TestMultiMapProxy_Size(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey+strconv.Itoa(i), testValue+strconv.Itoa(i))
	}
	size, err := multiMap.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(10), "multiMap Size() returned a wrong value")
}

func TestMultiMapProxy_ValueCount(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey, testValue+strconv.Itoa(i))
	}
	size, err := multiMap.ValueCount(testKey)
	require.NoError(t, err)
	assert.Equalf(t, size, int32(10), "multiMap ValueCount() returned a wrong value")
}

func TestMultiMapProxy_ValueCountWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	for i := 0; i < 10; i++ {
		multiMap.Put(testKey, testValue+strconv.Itoa(i))
	}
	_, err := multiMap.ValueCount(nil)
	require.Errorf(t, err, "multiMap ValueCount() failed ")
}

func TestMultiMapProxy_Values(t *testing.T) {
	defer multiMap.Clear()
	var expecteds = make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		multiMap.Put(strconv.Itoa(i), strconv.Itoa(i))
		expecteds[i] = strconv.Itoa(i)
	}
	values, err := multiMap.Values()
	require.NoError(t, err)
	assert.ElementsMatchf(t, values, expecteds, "multiMap Values() failed")
}

func TestMultiMapProxy_KeySet(t *testing.T) {
	var expecteds = make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		multiMap.Put(strconv.Itoa(i), int32(i))
		expecteds[i] = strconv.Itoa(i)
	}
	keySlice, err := multiMap.KeySet()
	require.NoError(t, err)
	assert.ElementsMatchf(t, keySlice, expecteds, "multiMap KeySet() failed")
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
	require.NoError(t, err)
	assert.ElementsMatchf(t, resultKeys, expectedKeys, "multiMap EntrySet() failed")
	require.NoError(t, err)
	assert.ElementsMatchf(t, resultValues, expectedValues, "multiMap EntrySet() failed")
}

func TestMultiMapProxy_AddEntryListener_IllegalListener(t *testing.T) {
	_, err := multiMap.AddEntryListener(5, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("MultiMap.AddEntryListener should return HazelcastIllegalArgumentError")
	}
}

func TestMultiMapProxy_AddEntryListenerAdded(t *testing.T) {
	defer multiMap.Clear()
	var wg = new(sync.WaitGroup)
	entryListener := &EntryListener{wg: wg}
	registrationID, err := multiMap.AddEntryListener(entryListener, true)
	defer multiMap.RemoveEntryListener(registrationID)
	require.NoError(t, err)
	wg.Add(1)
	multiMap.Put(testKey, testValue)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "multiMap AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Key(), testKey, "multiMap AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.Value(), testValue, "multiMap AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.OldValue(), nil, "multiMap AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.MergingValue(), nil, "multiMap AddEntryListener entryAdded failed")
	assert.Equalf(t, entryListener.event.EventType(), int32(1), "multiMap AddEntryListener entryAdded failed")

}

func TestMultiMapProxy_AddEntryListenerToKey_IllegalListener(t *testing.T) {
	_, err := multiMap.AddEntryListenerToKey(5, nil, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("MultiMap.AddEntryListenerToKey should return HazelcastIllegalArgumentError")
	}
}

func TestMultiMapProxy_EntryListenerToKey(t *testing.T) {
	defer multiMap.Clear()
	var wg = new(sync.WaitGroup)
	entryListener := &EntryListener{wg: wg}
	registrationID, err := multiMap.AddEntryListenerToKey(entryListener, "key1", true)
	defer multiMap.RemoveEntryListener(registrationID)
	require.NoError(t, err)
	wg.Add(1)
	multiMap.Put("key1", "value1")
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "AddEntryListenerToKey failed")
	assert.Equalf(t, entryListener.event.Key(), "key1", "multiMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, entryListener.event.Value(), "value1", "multiMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, entryListener.event.OldValue(), nil, "multiMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, entryListener.event.MergingValue(), nil, "multiMap AddEntryListenerToKey entryAdded failed")
	assert.Equalf(t, entryListener.event.EventType(), int32(1), "multiMap AddEntryListenerToKey entryAdded failed")
	wg.Add(1)
	multiMap.Put("key2", "value1")
	timeout = test.WaitTimeout(wg, test.Timeout/20)
	assert.Equalf(t, true, timeout, "multiMap AddEntryListenerToKey failed")
}

func TestMultiMapProxy_LockWithLeaseTime(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.LockWithLeaseTime(testKey, 1*time.Millisecond)
	test.AssertEventually(t, func() bool {
		locked, err := multiMap.IsLocked(testKey)
		return err == nil && locked == false
	})
}

func TestMultiMapProxy_LockWithLeaseTimeWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	err := multiMap.LockWithLeaseTime(nil, 10*time.Millisecond)
	require.Errorf(t, err, "multiMap LockWithLeaseTime() failed ")
}

func TestMultiMapProxy_Lock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	multiMap.LockWithLeaseTime(testKey, 1*time.Millisecond)
	test.AssertEventually(t, func() bool {
		locked, err := multiMap.IsLocked(testKey)
		return err == nil && locked == false
	})
}

func TestMultiMapProxy_IsLocked(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	locked, err := multiMap.IsLocked(testKey)
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "multiMap IsLock() failed.")
	err = multiMap.Lock(testKey)
	if err != nil {
		t.Fatal(err)
	}
	locked, err = multiMap.IsLocked(testKey)
	require.NoError(t, err)
	assert.Equalf(t, locked, true, "multiMap Lock() failed")
	err = multiMap.Unlock(testKey)
	if err != nil {
		t.Error(err)
	}
	locked, err = multiMap.IsLocked(testKey)
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "multiMap Lock() failed")
}

func TestMultiMapProxy_IsLockedWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.IsLocked(nil)
	require.Errorf(t, err, "multiMap IsLocked() failed")
}

func TestMultiMapProxy_TryLock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	defer multiMap.ForceUnlock(testKey)
	ok, err := multiMap.TryLockWithTimeoutAndLease(testKey, 1*time.Second, 2*time.Second)
	require.NoError(t, err)
	assert.Equalf(t, ok, true, "multiMap TryLock() failed.")
	test.AssertEventually(t, func() bool {
		locked, err := multiMap.IsLocked(testKey)
		return err == nil && locked == false
	})
}

func TestMultiMapProxy_TryLockWithNilKey(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	_, err := multiMap.TryLockWithTimeoutAndLease(nil, 1*time.Second, 2*time.Second)
	require.Errorf(t, err, "multiMap TryLock() failed")
}

func TestMultiMapProxy_ForceUnlock(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	ok, err := multiMap.TryLockWithTimeoutAndLease(testKey, 1*time.Second, 20*time.Second)
	require.NoError(t, err)
	assert.Equalf(t, ok, true, "multiMap ForceUnLock() failed.")
	multiMap.ForceUnlock(testKey)
	locked, err := multiMap.IsLocked(testKey)
	defer multiMap.Unlock(testKey)
	require.NoError(t, err)
	assert.Equalf(t, locked, false, "multiMap ForceUnLock() failed.")
}

func TestMultiMapProxy_ForceUnlockWithNil(t *testing.T) {
	defer multiMap.Clear()
	multiMap.Put(testKey, testValue)
	err := multiMap.ForceUnlock(nil)
	require.Errorf(t, err, "multiMap ForceUnlock() failed")

}

type EntryListener struct {
	wg    *sync.WaitGroup
	event core.EntryEvent
}

func (l *EntryListener) EntryAdded(event core.EntryEvent) {
	l.event = event
	l.wg.Done()
}

func (l *EntryListener) EntryRemoved(event core.EntryEvent) {
	l.wg.Done()
}

func (l *EntryListener) EntryEvicted(event core.EntryEvent) {
	l.wg.Done()
}

func (l *EntryListener) MapCleared(event core.MapEvent) {
	l.wg.Done()
}
