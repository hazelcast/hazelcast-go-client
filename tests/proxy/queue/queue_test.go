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

package queue

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"sync"
	"testing"
	"time"
)

var queue core.IQueue
var client hazelcast.IHazelcastInstance
var testElement = "testElement"
var queueName = "ClientQueueTest"

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	queue, _ = client.GetQueue(queueName)
	m.Run()
	queue.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestQueueProxy_AddAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	added, err := queue.AddAll(all)
	items, err2 := queue.ToSlice()
	AssertEqualf(t, err, added, true, "queue AddAll() failed")
	AssertEqualf(t, err2, items[0], all[0], "queue AddAll() failed")
	AssertEqualf(t, err2, items[1], all[1], "queue AddAll() failed")
}

func TestQueueProxy_AddAllWithNilItems(t *testing.T) {
	defer queue.Clear()
	_, err := queue.AddAll(nil)
	AssertErrorNotNil(t, err, "queue AddAll() did not return an error for nil items")

}

func TestQueueProxy_Clear(t *testing.T) {
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	queue.Clear()
	size, err := queue.Size()
	AssertEqualf(t, err, size, int32(0), "queue Clear() should clear the queue")
}

func TestQueueProxy_Contains(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	found, err := queue.Contains("1")
	AssertEqualf(t, err, found, true, "queue Contains() failed")
}

func TestQueueProxy_ContainsWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.Contains(nil)
	AssertErrorNotNil(t, err, "queue Contains() should return error for nil element")
}

func TestQueueProxy_ContainsAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	found, err := queue.ContainsAll(all)
	AssertEqualf(t, err, found, true, "queue ContainsAll() failed")
}

func TestQueueProxy_ContainsAllNilSlice(t *testing.T) {
	defer queue.Clear()
	_, err := queue.ContainsAll(nil)
	AssertErrorNotNil(t, err, "queue ContainsAll() should return error for nil slice")
}

func TestQueueProxy_DrainTo(t *testing.T) {
	defer queue.Clear()
	queue.AddAll([]interface{}{"2"})
	drainSlice := make([]interface{}, 0)
	drainSlice = append(drainSlice, "0", "1")
	movedAmount, err := queue.DrainTo(&drainSlice)
	AssertEqualf(t, err, movedAmount, int32(1), "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[0], "0", "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[1], "1", "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[2], "2", "queue DrainTo() failed")
}

func TestQueueProxy_DrainToWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.DrainTo(nil)
	AssertErrorNotNil(t, err, "queue DrainTo() should return error for nil element")
}

func TestQueueProxy_DrainToWithMaxSize(t *testing.T) {
	defer queue.Clear()
	queue.AddAll([]interface{}{"2", "2", "2"})
	drainSlice := make([]interface{}, 0)
	drainSlice = append(drainSlice, "0", "1")
	movedAmount, err := queue.DrainToWithMaxSize(&drainSlice, 1)
	AssertEqualf(t, err, movedAmount, int32(1), "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[0], "0", "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[1], "1", "queue DrainTo() failed")
	AssertEqualf(t, err, drainSlice[2], "2", "queue DrainTo() failed")
}

func TestQueueProxy_DrainToWithMaxSizeWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.DrainToWithMaxSize(nil, 5)
	AssertErrorNotNil(t, err, "queue DrainToWithMaxSize() should return error for nil element")
}

func TestQueueProxy_IsEmpty(t *testing.T) {
	defer queue.Clear()
	empty, err := queue.IsEmpty()
	AssertEqualf(t, err, empty, true, "queue IsEmpty() failed")
}

func TestQueueProxy_Offer(t *testing.T) {
	defer queue.Clear()
	changed, err := queue.Offer(testElement)
	AssertEqualf(t, err, changed, true, "queue Offer() failed")
	result, err := queue.Peek()
	AssertEqualf(t, err, result, testElement, "queue Offer() failed")
}

func TestQueueProxy_OfferWithFullCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3", "4", "5", "6"}
	queue.AddAll(all)
	changed, err := queue.Offer(testElement)
	AssertEqualf(t, err, changed, false, "queue Offer() failed with full capacity")
}

func TestQueueProxy_OfferWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.Offer(nil)
	AssertErrorNotNil(t, err, "queue Offer() should return error for nil element")
}

func TestQueueProxy_OfferWithTimeout(t *testing.T) {
	defer queue.Clear()
	changed, err := queue.OfferWithTimeout(testElement, 0, time.Millisecond)
	AssertEqualf(t, err, changed, true, "queue Offer() failed")
	result, err := queue.Peek()
	AssertEqualf(t, err, result, testElement, "queue Offer() failed")
}

func TestQueueProxy_OfferWithTimeoutWithFullCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3", "4", "5", "6"}
	queue.AddAll(all)
	changed, err := queue.OfferWithTimeout(testElement, 1000, time.Millisecond)
	AssertEqualf(t, err, changed, false, "queue Offer() failed with full capacity")
}

func TestQueueProxy_OfferWithTimeoutWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.OfferWithTimeout(nil, 0, time.Millisecond)
	AssertErrorNotNil(t, err, "queue OfferWithTimeout() should return error for nil element")
}

func TestQueueProxy_Peek(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Peek()
	AssertEqualf(t, err, item, testElement, "queue Peek() failed")
}

func TestQueueProxy_PeekEMptyQueue(t *testing.T) {
	defer queue.Clear()
	item, err := queue.Peek()
	AssertEqualf(t, err, item, nil, "queue Peek() failed")
}

func TestQueueProxy_Poll(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Poll()
	AssertEqualf(t, err, item, testElement, "queue Poll() failed")
}

func TestQueueProxy_PollEmpty(t *testing.T) {
	defer queue.Clear()
	item, err := queue.Poll()
	AssertEqualf(t, err, item, nil, "queue Poll() failed")
}

func TestQueueProxy_PollWithTimeout(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.PollWithTimeout(1000, time.Millisecond)
	AssertEqualf(t, err, item, testElement, "queue PollWithTimeout() failed")
}

func TestQueueProxy_PollWithTimeoutEmpty(t *testing.T) {
	defer queue.Clear()
	item, err := queue.PollWithTimeout(1000, time.Millisecond)
	AssertEqualf(t, err, item, nil, "queue PollWithTimeout() failed")
}

func TestQueueProxy_Put(t *testing.T) {
	defer queue.Clear()
	err := queue.Put(testElement)
	AssertEqualf(t, err, nil, nil, "queue Put() failed")
	result, err := queue.Peek()
	AssertEqualf(t, err, result, testElement, "queue Put() failed")
}

func TestQueueProxy_RemainingCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	remainingCapacity1, err1 := queue.RemainingCapacity()
	queue.AddAll(all)
	remainingCapacity2, err2 := queue.RemainingCapacity()
	AssertEqualf(t, err1, remainingCapacity1, int32(6), "queue RemainingCapacity() failed")
	AssertEqualf(t, err2, remainingCapacity2, int32(3), "queue RemainingCapacity() failed")
}

func TestQueueProxy_Remove(t *testing.T) {
	defer queue.Clear()
	queue.Put("1")
	removed, err := queue.Remove("1")
	AssertEqualf(t, err, removed, true, "queue Remove() failed")
}

func TestQueueProxy_RemoveWithNilElement(t *testing.T) {
	defer queue.Clear()
	queue.Put("1")
	_, err := queue.Remove(nil)
	AssertErrorNotNil(t, err, "queue Remove() failed")
}

func TestQueueProxy_RemoveEmpty(t *testing.T) {
	defer queue.Clear()
	removed, err := queue.Remove("1")
	AssertEqualf(t, err, removed, false, "queue Remove() failed")
}

func TestQueueProxy_RemoveAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	removedAll, err := queue.RemoveAll([]interface{}{"2", "3"})
	AssertEqualf(t, err, removedAll, true, "queue RemoveAll() failed")
	found, err := queue.Contains("1")
	AssertEqualf(t, err, found, true, "queue RemoveAll() failed")
}

func TestQueueProxy_RemoveAllWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RemoveAll(nil)
	AssertErrorNotNil(t, err, "queue RemoveAll() failed")
}

func TestQueueProxy_RemoveAllEmpty(t *testing.T) {
	defer queue.Clear()
	removedAll, err := queue.RemoveAll([]interface{}{"2", "3"})
	AssertEqualf(t, err, removedAll, false, "queue RemoveAll() failed")
}

func TestQueueProxy_RetainAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	changed, err := queue.RetainAll([]interface{}{"2", "3"})
	AssertEqualf(t, err, changed, true, "queue RetainAll() failed")
	found, err := queue.Contains("1")
	AssertEqualf(t, err, found, false, "queue RetainAll() failed")
}

func TestQueueProxy_RetainAllWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RetainAll([]interface{}{nil, "1", "2"})
	AssertErrorNotNil(t, err, "queue RetainAll() should return error with nil element")
}

func TestQueueProxy_RetainAllWithNilSlice(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RetainAll(nil)
	AssertErrorNotNil(t, err, "queue RetainAll() should return error with nil slice")
}

func TestQueueProxy_Size(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	size, err := queue.Size()
	AssertEqualf(t, err, size, int32(3), "queue Size() failed")
}

func TestQueueProxy_Take(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Take()
	AssertEqualf(t, err, item, testElement, "queue Take() failed")
}

func TestQueueProxy_ToSlice(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	res, err := queue.ToSlice()
	AssertEqualf(t, err, res[0], all[0], "queue ToSlice() failed")
	AssertEqualf(t, err, res[1], all[1], "queue ToSlice() failed")
}

func TestListProxy_AddItemListenerItemAddedIncludeValue(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, true)
	defer queue.RemoveItemListener(registrationID)
	AssertNilf(t, err, nil, "queue AddItemListener() failed when item is added")
	queue.Put(testElement)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "queue AddItemListener() failed when item is added")
	AssertEqualf(t, nil, listener.event.Item(), testElement, "queue AddItemListener() failed when item is added")
	AssertEqualf(t, nil, listener.event.EventType(), int32(1), "queue AddItemListener() failed when item is added")
	AssertEqualf(t, nil, listener.event.Name(), queueName, "queue AddItemListener() failed when item is added")
}

func TestListProxy_AddItemItemAddedListener(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	AssertNilf(t, err, nil, "queue AddItemListener() failed when item is added")
	queue.Put(testElement)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "queue AddItemListener() failed when item is added")
	AssertEqualf(t, nil, listener.event.Item(), nil, "queue AddItemListener() failed when item is added")
}

func TestListProxy_AddItemListenerItemRemovedIncludeValue(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	queue.Put(testElement)
	registrationID, err := queue.AddItemListener(listener, true)
	defer queue.RemoveItemListener(registrationID)
	AssertNilf(t, err, nil, "queue AddItemListener() failed when item is removed")
	queue.Remove(testElement)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "queue AddItemListenerItemRemoved() failed when item is removed")
	AssertEqualf(t, nil, listener.event.Item(), testElement, "queue AddItemListener() failed when item is removed")
	AssertEqualf(t, nil, listener.event.EventType(), int32(2), "queue AddItemListener() failed when item is removed")
	AssertEqualf(t, nil, listener.event.Name(), queueName, "queue AddItemListener() failed when item is removed")
}

func TestListProxy_AddItemListenerItemRemoved(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	queue.Put(testElement)
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	AssertNilf(t, err, nil, "queue AddItemListener() failed when item is removed")
	queue.Remove(testElement)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "queue AddItemListenerItemRemoved() failed when item is removed")
	AssertEqualf(t, nil, listener.event.Item(), nil, "queue AddItemListener() failed when item is removed")
}

func TestListProxy_AddItemItemRemovedListener(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	AssertNilf(t, err, nil, "queue AddItemListener() failed")
	queue.Put(testElement)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "queue AddItemListener() failed")
	AssertEqualf(t, nil, listener.event.Item(), nil, "queue AddItemListener() failed")
}

type itemListener struct {
	wg    *sync.WaitGroup
	event core.IItemEvent
}

func (self *itemListener) ItemAdded(event core.IItemEvent) {
	self.event = event
	self.wg.Done()
}

func (self *itemListener) ItemRemoved(event core.IItemEvent) {
	self.event = event
	self.wg.Done()
}
