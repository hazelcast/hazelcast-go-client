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

package memberdown

import (
	"log"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

var client hazelcast.Instance
var mp core.Map

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	mp, _ = client.GetMap("myMap")
	remoteController.ShutdownCluster(cluster.ID)
	m.Run()
	mp.Clear()
	client.Shutdown()
}

func TestMapPutWhenMemberDown(t *testing.T) {
	_, err := mp.Put("key", "value")
	assert.ErrorNotNil(t, err, "put should have returned an error when member is down")
}

func TestMapGetWhenMemberDown(t *testing.T) {
	_, err := mp.Get("key")
	assert.ErrorNotNil(t, err, "get should have returned an error when member is down")
}

func TestMapRemoveWhenMemberDown(t *testing.T) {
	_, err := mp.Remove("key")
	assert.ErrorNotNil(t, err, "remove should have returned an error when member is down")
}

func TestMapRemoveIfSameWhenMemberDown(t *testing.T) {
	_, err := mp.RemoveIfSame("key", "value")
	assert.ErrorNotNil(t, err, "removeIfSame should have returned an error when member is down")
}

func TestMapRemoveAllWhenMemberDown(t *testing.T) {
	err := mp.RemoveAll(predicate.GreaterThan("this", int32(40)))
	assert.ErrorNotNil(t, err, "removeAll should have returned an error when member is down")
}

func TestMapSizeWhenMemberDown(t *testing.T) {
	_, err := mp.Size()
	assert.ErrorNotNil(t, err, "size should have returned an error when member is down")
}

func TestMapContainsKeyWhenMemberDown(t *testing.T) {
	_, err := mp.ContainsKey("key")
	assert.ErrorNotNil(t, err, "containsKey should have returned an error when member is down")
}

func TestMapContainsValueWhenMemberDown(t *testing.T) {
	_, err := mp.ContainsValue("value")
	assert.ErrorNotNil(t, err, "containsValue should have returned an error when member is down")
}

func TestMapClearWhenMemberDown(t *testing.T) {
	err := mp.Clear()
	assert.ErrorNotNil(t, err, "clear should have returned an error when member is down")
}

func TestMapDeleteWhenMemberDown(t *testing.T) {
	err := mp.Delete("key")
	assert.ErrorNotNil(t, err, "delete should have returned an error when member is down")
}

func TestMapIsEmptyWhenMemberDown(t *testing.T) {
	_, err := mp.IsEmpty()
	assert.ErrorNotNil(t, err, "isEmpty should have returned an error when member is down")
}

func TestMapAddIndexWhenMemberDown(t *testing.T) {
	err := mp.AddIndex("attribute", false)
	assert.ErrorNotNil(t, err, "addIndex should have returned an error when member is down")
}

func TestMapEvictWhenMemberDown(t *testing.T) {
	_, err := mp.Evict("key")
	assert.ErrorNotNil(t, err, "evict should have returned an error when member is down")
}

func TestMapEvictAllWhenMemberDown(t *testing.T) {
	err := mp.EvictAll()
	assert.ErrorNotNil(t, err, "evictAll should have returned an error when member is down")
}

func TestMapFlushWhenMemberDown(t *testing.T) {
	err := mp.Flush()
	assert.ErrorNotNil(t, err, "flush should have returned an error when member is down")
}

func TestMapForceUnlockWhenMemberDown(t *testing.T) {
	err := mp.ForceUnlock("key")
	assert.ErrorNotNil(t, err, "forceUnlock should have returned an error when member is down")
}

func TestMapLockWhenMemberDown(t *testing.T) {
	err := mp.Lock("key")
	assert.ErrorNotNil(t, err, "lock should have returned an error when member is down")
}

func TestMapLockWithLeaseTimeWhenMemberDown(t *testing.T) {
	err := mp.LockWithLeaseTime("key", 3*time.Second)
	assert.ErrorNotNil(t, err, "lockWithLeaseTime should have returned an error when member is down")
}

func TestMapUnlockWhenMemberDown(t *testing.T) {
	err := mp.Unlock("key")
	assert.ErrorNotNil(t, err, "unlock should have returned an error when member is down")
}

func TestMapIsLockedWhenMemberDown(t *testing.T) {
	_, err := mp.IsLocked("key")
	assert.ErrorNotNil(t, err, "isLocked should have returned an error when member is down")
}

func TestMapReplaceWhenMemberDown(t *testing.T) {
	_, err := mp.Replace("key", "value")
	assert.ErrorNotNil(t, err, "replace should have returned an error when member is down")
}

func TestMapReplaceIfSameWhenMemberDown(t *testing.T) {
	_, err := mp.ReplaceIfSame("key", "oldvalue", "newValue")
	assert.ErrorNotNil(t, err, "replaceIfSame should have returned an error when member is down")
}

func TestMapSetWhenMemberDown(t *testing.T) {
	err := mp.Set("key", "value")
	assert.ErrorNotNil(t, err, "set should have returned an error when member is down")
}

func TestMapSetWithTTLWhenMemberDown(t *testing.T) {
	err := mp.SetWithTTL("key", "value", 2*time.Second)
	assert.ErrorNotNil(t, err, "setWithTTL should have returned an error when member is down")
}

func TestMapPutIfAbsentWhenMemberDown(t *testing.T) {
	_, err := mp.PutIfAbsent("key", "value")
	assert.ErrorNotNil(t, err, "putIfAbsent should have returned an error when member is down")
}

func TestMapPutAllWhenMemberDown(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	testMap[1] = 2
	err := mp.PutAll(testMap)
	assert.ErrorNotNil(t, err, "putAll should have returned an error when member is down")
}

func TestMapKeySetWhenMemberDown(t *testing.T) {
	_, err := mp.KeySet()
	assert.ErrorNotNil(t, err, "keySet should have returned an error when member is down")
}

func TestMapKeySetWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.KeySetWithPredicate(predicate.GreaterThan("this", 5))
	assert.ErrorNotNil(t, err, "keySetWithPredicate should have returned an error when member is down")
}

func TestMapValuesWhenMemberDown(t *testing.T) {
	_, err := mp.Values()
	assert.ErrorNotNil(t, err, "values should have returned an error when member is down")
}

func TestMapValuesWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.ValuesWithPredicate(predicate.GreaterThan("this", 5))
	assert.ErrorNotNil(t, err, "valuesWithPredicate should have returned an error when member is down")
}

func TestMapEntrySetWhenMemberDown(t *testing.T) {
	_, err := mp.EntrySet()
	assert.ErrorNotNil(t, err, "entrySet should have returned an error when member is down")
}

func TestMapEntrySetWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.EntrySetWithPredicate(predicate.GreaterThan("this", 5))
	assert.ErrorNotNil(t, err, "entrySetWithPredicate should have returned an error when member is down")
}

func TestMapTryLockWhenMemberDown(t *testing.T) {
	_, err := mp.TryLock("key")
	assert.ErrorNotNil(t, err, "tryLock should have returned an error when member is down")
}

func TestMapTryLockWithTimeoutWhenMemberDown(t *testing.T) {
	_, err := mp.TryLockWithTimeout("key", 1*time.Second)
	assert.ErrorNotNil(t, err, "tryLockWithTimeout should have returned an error when member is down")
}

func TestMapTryLockWithTimeoutAndLeaseWhenMemberDown(t *testing.T) {
	_, err := mp.TryLockWithTimeoutAndLease("key", 2*time.Second, 2*time.Second)
	assert.ErrorNotNil(t, err, "tryLockWithTimeoutAndLease should have returned an error when member is down")

}

func TestMapTryPutWhenMemberDown(t *testing.T) {
	_, err := mp.TryPut("key", "value")
	assert.ErrorNotNil(t, err, "tryPut should have returned an error when member is down")
}

func TestMapTryRemoveWhenMemberDown(t *testing.T) {
	_, err := mp.TryRemove("key", 2*time.Second)
	assert.ErrorNotNil(t, err, "tryRemove should have returned an error when member is down")
}

func TestMapGetAllWhenMemberDown(t *testing.T) {
	testSlice := make([]interface{}, 2)
	testSlice[0] = 2
	_, err := mp.GetAll(testSlice)
	assert.ErrorNotNil(t, err, "getAll should have returned an error when member is down")
}

func TestMapGetEntryViewWhenMemberDown(t *testing.T) {
	_, err := mp.GetEntryView("key")
	assert.ErrorNotNil(t, err, "getEntryView should have returned an error when member is down")
}

func TestMapPutTransientWhenMemberDown(t *testing.T) {
	err := mp.PutTransient("key", "value", 2*time.Second)
	assert.ErrorNotNil(t, err, "putTransient should have returned an error when member is down")
}

func TestMapAddEntryListenerWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListener(nil, false)
	assert.ErrorNotNil(t, err, "entryListener should have returned an error when member is down")
}

func TestMapAddEntryListenerToKeyWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerToKey(nil, "key", false)
	assert.ErrorNotNil(t, err, "entryListenerToKey should have returned an error when member is down")
}

func TestMapAddEntryListenerToKeyWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(nil, predicate.GreaterThan("this", 5), "key", false)
	assert.ErrorNotNil(t, err, "entryListenerToKeyWithPredicate should have returned an error when member is down")
}

func TestMapAddEntryListenerWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerWithPredicate(nil, predicate.GreaterThan("this", 5), false)
	assert.ErrorNotNil(t, err, "entryListenerWithPredicate should have returned an error when member is down")
}

func TestMapExecuteOnKeyWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnKey("key", nil)
	assert.ErrorNotNil(t, err, "executeOnKey should have returned an error when member is down")
}

func TestMapExecuteOnEntriesWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnEntriesWithPredicate(nil, predicate.GreaterThan("this", 5))
	assert.ErrorNotNil(t, err, "executeOnEntriesWithPredicate should have returned an error when member is down")
}

func TestMapExecuteOnEntriesWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnEntries(nil)
	assert.ErrorNotNil(t, err, "executeOnEntries should have returned an error when member is down")
}

func TestMapExecuteOnKeysWhenMemberDown(t *testing.T) {
	testSlice := make([]interface{}, 2)
	testSlice[0] = 1
	_, err := mp.ExecuteOnKeys(testSlice, nil)
	assert.ErrorNotNil(t, err, "executeOnKey should have returned an error when member is down")
}
