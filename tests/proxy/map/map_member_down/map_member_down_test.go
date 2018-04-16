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

package map_member_down

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicates"
	"github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"testing"
	"time"
)

var client hazelcast.IHazelcastInstance
var mp core.IMap

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
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
	AssertErrorNotNil(t, err, "put should have returned an error when member is down")
}

func TestMapGetWhenMemberDown(t *testing.T) {
	_, err := mp.Get("key")
	AssertErrorNotNil(t, err, "get should have returned an error when member is down")
}

func TestMapRemoveWhenMemberDown(t *testing.T) {
	_, err := mp.Remove("key")
	AssertErrorNotNil(t, err, "remove should have returned an error when member is down")
}

func TestMapRemoveIfSameWhenMemberDown(t *testing.T) {
	_, err := mp.RemoveIfSame("key", "value")
	AssertErrorNotNil(t, err, "removeIfSame should have returned an error when member is down")
}

func TestMapRemoveAllWhenMemberDown(t *testing.T) {
	err := mp.RemoveAll(predicates.GreaterThan("this", int32(40)))
	AssertErrorNotNil(t, err, "removeAll should have returned an error when member is down")
}

func TestMapSizeWhenMemberDown(t *testing.T) {
	_, err := mp.Size()
	AssertErrorNotNil(t, err, "size should have returned an error when member is down")
}

func TestMapContainsKeyWhenMemberDown(t *testing.T) {
	_, err := mp.ContainsKey("key")
	AssertErrorNotNil(t, err, "containsKey should have returned an error when member is down")
}

func TestMapContainsValueWhenMemberDown(t *testing.T) {
	_, err := mp.ContainsValue("value")
	AssertErrorNotNil(t, err, "containsValue should have returned an error when member is down")
}

func TestMapClearWhenMemberDown(t *testing.T) {
	err := mp.Clear()
	AssertErrorNotNil(t, err, "clear should have returned an error when member is down")
}

func TestMapDeleteWhenMemberDown(t *testing.T) {
	err := mp.Delete("key")
	AssertErrorNotNil(t, err, "delete should have returned an error when member is down")
}

func TestMapIsEmptyWhenMemberDown(t *testing.T) {
	_, err := mp.IsEmpty()
	AssertErrorNotNil(t, err, "isEmpty should have returned an error when member is down")
}

func TestMapAddIndexWhenMemberDown(t *testing.T) {
	err := mp.AddIndex("attribute", false)
	AssertErrorNotNil(t, err, "addIndex should have returned an error when member is down")
}

func TestMapEvictWhenMemberDown(t *testing.T) {
	_, err := mp.Evict("key")
	AssertErrorNotNil(t, err, "evict should have returned an error when member is down")
}

func TestMapEvictAllWhenMemberDown(t *testing.T) {
	err := mp.EvictAll()
	AssertErrorNotNil(t, err, "evictAll should have returned an error when member is down")
}

func TestMapFlushWhenMemberDown(t *testing.T) {
	err := mp.Flush()
	AssertErrorNotNil(t, err, "flush should have returned an error when member is down")
}

func TestMapForceUnlockWhenMemberDown(t *testing.T) {
	err := mp.ForceUnlock("key")
	AssertErrorNotNil(t, err, "forceUnlock should have returned an error when member is down")
}

func TestMapLockWhenMemberDown(t *testing.T) {
	err := mp.Lock("key")
	AssertErrorNotNil(t, err, "lock should have returned an error when member is down")
}

func TestMapLockWithLeaseTimeWhenMemberDown(t *testing.T) {
	err := mp.LockWithLeaseTime("key", 3, time.Second)
	AssertErrorNotNil(t, err, "lockWithLeaseTime should have returned an error when member is down")
}

func TestMapUnlockWhenMemberDown(t *testing.T) {
	err := mp.Unlock("key")
	AssertErrorNotNil(t, err, "unlock should have returned an error when member is down")
}

func TestMapIsLockedWhenMemberDown(t *testing.T) {
	_, err := mp.IsLocked("key")
	AssertErrorNotNil(t, err, "isLocked should have returned an error when member is down")
}

func TestMapReplaceWhenMemberDown(t *testing.T) {
	_, err := mp.Replace("key", "value")
	AssertErrorNotNil(t, err, "replace should have returned an error when member is down")
}

func TestMapReplaceIfSameWhenMemberDown(t *testing.T) {
	_, err := mp.ReplaceIfSame("key", "oldvalue", "newValue")
	AssertErrorNotNil(t, err, "replaceIfSame should have returned an error when member is down")
}

func TestMapSetWhenMemberDown(t *testing.T) {
	err := mp.Set("key", "value")
	AssertErrorNotNil(t, err, "set should have returned an error when member is down")
}

func TestMapSetWithTtlWhenMemberDown(t *testing.T) {
	err := mp.SetWithTtl("key", "value", 2, time.Second)
	AssertErrorNotNil(t, err, "setWithTtl should have returned an error when member is down")
}

func TestMapPutIfAbsentWhenMemberDown(t *testing.T) {
	_, err := mp.PutIfAbsent("key", "value")
	AssertErrorNotNil(t, err, "putIfAbsent should have returned an error when member is down")
}

func TestMapPutAllWhenMemberDown(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	testMap[1] = 2
	err := mp.PutAll(testMap)
	AssertErrorNotNil(t, err, "putAll should have returned an error when member is down")
}

func TestMapKeySetWhenMemberDown(t *testing.T) {
	_, err := mp.KeySet()
	AssertErrorNotNil(t, err, "keySet should have returned an error when member is down")
}

func TestMapKeySetWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.KeySetWithPredicate(predicates.GreaterThan("this", 5))
	AssertErrorNotNil(t, err, "keySetWithPredicate should have returned an error when member is down")
}

func TestMapValuesWhenMemberDown(t *testing.T) {
	_, err := mp.Values()
	AssertErrorNotNil(t, err, "values should have returned an error when member is down")
}

func TestMapValuesWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.ValuesWithPredicate(predicates.GreaterThan("this", 5))
	AssertErrorNotNil(t, err, "valuesWithPredicate should have returned an error when member is down")
}

func TestMapEntrySetWhenMemberDown(t *testing.T) {
	_, err := mp.EntrySet()
	AssertErrorNotNil(t, err, "entrySet should have returned an error when member is down")
}

func TestMapEntrySetWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.EntrySetWithPredicate(predicates.GreaterThan("this", 5))
	AssertErrorNotNil(t, err, "entrySetWithPredicate should have returned an error when member is down")
}

func TestMapTryLockWhenMemberDown(t *testing.T) {
	_, err := mp.TryLock("key")
	AssertErrorNotNil(t, err, "tryLock should have returned an error when member is down")
}

func TestMapTryLockWithTimeoutWhenMemberDown(t *testing.T) {
	_, err := mp.TryLockWithTimeout("key", 1, time.Second)
	AssertErrorNotNil(t, err, "tryLockWithTimeout should have returned an error when member is down")
}

func TestMapTryLockWithTimeoutAndLeaseWhenMemberDown(t *testing.T) {
	_, err := mp.TryLockWithTimeoutAndLease("key", 2, time.Second, 2, time.Second)
	AssertErrorNotNil(t, err, "tryLockWithTimeoutAndLease should have returned an error when member is down")
}

func TestMapTryPutWhenMemberDown(t *testing.T) {
	_, err := mp.TryPut("key", "value")
	AssertErrorNotNil(t, err, "tryPut should have returned an error when member is down")
}

func TestMapTryRemoveWhenMemberDown(t *testing.T) {
	_, err := mp.TryRemove("key", 2, time.Second)
	AssertErrorNotNil(t, err, "tryRemove should have returned an error when member is down")
}

func TestMapGetAllWhenMemberDown(t *testing.T) {
	testSlice := make([]interface{}, 2)
	testSlice[0] = 2
	_, err := mp.GetAll(testSlice)
	AssertErrorNotNil(t, err, "getAll should have returned an error when member is down")
}

func TestMapGetEntryViewWhenMemberDown(t *testing.T) {
	_, err := mp.GetEntryView("key")
	AssertErrorNotNil(t, err, "getEntryView should have returned an error when member is down")
}

func TestMapPutTransientWhenMemberDown(t *testing.T) {
	err := mp.PutTransient("key", "value", 2, time.Second)
	AssertErrorNotNil(t, err, "putTransient should have returned an error when member is down")
}

func TestMapAddEntryListenerWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListener(nil, false)
	AssertErrorNotNil(t, err, "entryListener should have returned an error when member is down")
}

func TestMapAddEntryListenerToKeyWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerToKey(nil, "key", false)
	AssertErrorNotNil(t, err, "entryListenerToKey should have returned an error when member is down")
}

func TestMapAddEntryListenerToKeyWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerToKeyWithPredicate(nil, predicates.GreaterThan("this", 5), "key", false)
	AssertErrorNotNil(t, err, "entryListenerToKeyWithPredicate should have returned an error when member is down")
}

func TestMapAddEntryListenerWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.AddEntryListenerWithPredicate(nil, predicates.GreaterThan("this", 5), false)
	AssertErrorNotNil(t, err, "entryListenerWithPredicate should have returned an error when member is down")
}

func TestMapExecuteOnKeyWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnKey("key", nil)
	AssertErrorNotNil(t, err, "executeOnKey should have returned an error when member is down")
}

func TestMapExecuteOnEntriesWithPredicateWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnEntriesWithPredicate(nil, predicates.GreaterThan("this", 5))
	AssertErrorNotNil(t, err, "executeOnEntriesWithPredicate should have returned an error when member is down")
}

func TestMapExecuteOnEntriesWhenMemberDown(t *testing.T) {
	_, err := mp.ExecuteOnEntries(nil)
	AssertErrorNotNil(t, err, "executeOnEntries should have returned an error when member is down")
}

func TestMapExecuteOnKeysWhenMemberDown(t *testing.T) {
	testSlice := make([]interface{}, 2)
	testSlice[0] = 1
	_, err := mp.ExecuteOnKeys(testSlice, nil)
	AssertErrorNotNil(t, err, "executeOnKey should have returned an error when member is down")
}
