// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
	"log"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var queue core.Queue
var client hazelcast.Client
var testElement = "testElement"
var queueName = "ClientQueueTest"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
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
	require.NoError(t, err2)
	assert.Equalf(t, added, true, "queue AddAll() failed")
	require.NoError(t, err)
	assert.Equalf(t, items[0], all[0], "queue AddAll() failed")
	require.NoError(t, err)
	assert.Equalf(t, items[1], all[1], "queue AddAll() failed")
}

func TestQueueProxy_AddAllWithNilItems(t *testing.T) {
	defer queue.Clear()
	_, err := queue.AddAll(nil)
	require.Errorf(t, err, "queue AddAll() did not return an error for nil items")

}

func TestQueueProxy_Clear(t *testing.T) {
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	queue.Clear()
	size, err := queue.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "queue Clear() should clear the queue")
}

func TestQueueProxy_Contains(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	found, err := queue.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, true, "queue Contains() failed")
}

func TestQueueProxy_ContainsWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.Contains(nil)
	require.Errorf(t, err, "queue Contains() should return error for nil element")
}

func TestQueueProxy_ContainsAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	found, err := queue.ContainsAll(all)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "queue ContainsAll() failed")
}

func TestQueueProxy_ContainsAllNilSlice(t *testing.T) {
	defer queue.Clear()
	_, err := queue.ContainsAll(nil)
	require.Errorf(t, err, "queue ContainsAll() should return error for nil slice")
}

func TestQueueProxy_DrainTo(t *testing.T) {
	defer queue.Clear()
	queue.AddAll([]interface{}{"2"})
	drainSlice := make([]interface{}, 0)
	drainSlice = append(drainSlice, "0", "1")
	movedAmount, err := queue.DrainTo(&drainSlice)
	require.NoError(t, err)
	assert.Equalf(t, movedAmount, int32(1), "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[0], "0", "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[1], "1", "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[2], "2", "queue DrainTo() failed")
}

func TestQueueProxy_DrainToWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.DrainTo(nil)
	require.Errorf(t, err, "queue DrainTo() should return error for nil element")
}

func TestQueueProxy_DrainToWithMaxSize(t *testing.T) {
	defer queue.Clear()
	queue.AddAll([]interface{}{"2", "2", "2"})
	drainSlice := make([]interface{}, 0)
	drainSlice = append(drainSlice, "0", "1")
	movedAmount, err := queue.DrainToWithMaxSize(&drainSlice, 1)
	require.NoError(t, err)
	assert.Equalf(t, movedAmount, int32(1), "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[0], "0", "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[1], "1", "queue DrainTo() failed")
	require.NoError(t, err)
	assert.Equalf(t, drainSlice[2], "2", "queue DrainTo() failed")
}

func TestQueueProxy_DrainToWithMaxSizeWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.DrainToWithMaxSize(nil, 5)
	require.Errorf(t, err, "queue DrainToWithMaxSize() should return error for nil element")
}

func TestQueueProxy_IsEmpty(t *testing.T) {
	defer queue.Clear()
	empty, err := queue.IsEmpty()
	require.NoError(t, err)
	assert.Equalf(t, empty, true, "queue IsEmpty() failed")
}

func TestQueueProxy_Offer(t *testing.T) {
	defer queue.Clear()
	changed, err := queue.Offer(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "queue Offer() failed")
	result, err := queue.Peek()
	require.NoError(t, err)
	assert.Equalf(t, result, testElement, "queue Offer() failed")
}

func TestQueueProxy_OfferWithFullCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3", "4", "5", "6"}
	queue.AddAll(all)
	changed, err := queue.Offer(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, false, "queue Offer() failed with full capacity")
}

func TestQueueProxy_OfferWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.Offer(nil)
	require.Errorf(t, err, "queue Offer() should return error for nil element")
}

func TestQueueProxy_OfferWithTimeout(t *testing.T) {
	defer queue.Clear()
	changed, err := queue.OfferWithTimeout(testElement, 0)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "queue Offer() failed")
	result, err := queue.Peek()
	require.NoError(t, err)
	assert.Equalf(t, result, testElement, "queue Offer() failed")
}

func TestQueueProxy_OfferWithTimeoutWithFullCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3", "4", "5", "6"}
	queue.AddAll(all)
	changed, err := queue.OfferWithTimeout(testElement, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equalf(t, changed, false, "queue Offer() failed with full capacity")
}

func TestQueueProxy_OfferWithTimeoutWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.OfferWithTimeout(nil, 0)
	require.Errorf(t, err, "queue OfferWithTimeout() should return error for nil element")
}

func TestQueueProxy_Peek(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Peek()
	require.NoError(t, err)
	assert.Equalf(t, item, testElement, "queue Peek() failed")
}

func TestQueueProxy_PeekEMptyQueue(t *testing.T) {
	defer queue.Clear()
	item, err := queue.Peek()
	require.NoError(t, err)
	assert.Equalf(t, item, nil, "queue Peek() failed")
}

func TestQueueProxy_Poll(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Poll()
	require.NoError(t, err)
	assert.Equalf(t, item, testElement, "queue Poll() failed")
}

func TestQueueProxy_PollEmpty(t *testing.T) {
	defer queue.Clear()
	item, err := queue.Poll()
	require.NoError(t, err)
	assert.Equalf(t, item, nil, "queue Poll() failed")
}

func TestQueueProxy_PollWithTimeout(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.PollWithTimeout(1000 * time.Millisecond)
	require.NoError(t, err)
	assert.Equalf(t, item, testElement, "queue PollWithTimeout() failed")
}

func TestQueueProxy_PollWithTimeoutEmpty(t *testing.T) {
	defer queue.Clear()
	item, err := queue.PollWithTimeout(1000 * time.Millisecond)
	require.NoError(t, err)
	assert.Equalf(t, item, nil, "queue PollWithTimeout() failed")
}

func TestQueueProxy_Put(t *testing.T) {
	defer queue.Clear()
	err := queue.Put(testElement)
	require.NoError(t, err)
	result, err := queue.Peek()
	require.NoError(t, err)
	assert.Equalf(t, result, testElement, "queue Put() failed")
}

func TestQueueProxy_RemainingCapacity(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	remainingCapacity1, err := queue.RemainingCapacity()
	require.NoError(t, err)
	queue.AddAll(all)
	remainingCapacity2, err := queue.RemainingCapacity()
	require.NoError(t, err)
	assert.Equalf(t, remainingCapacity1, int32(6), "queue RemainingCapacity() failed")
	require.NoError(t, err)
	assert.Equalf(t, remainingCapacity2, int32(3), "queue RemainingCapacity() failed")
}

func TestQueueProxy_Remove(t *testing.T) {
	defer queue.Clear()
	queue.Put("1")
	removed, err := queue.Remove("1")
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "queue Remove() failed")
}

func TestQueueProxy_RemoveWithNilElement(t *testing.T) {
	defer queue.Clear()
	queue.Put("1")
	_, err := queue.Remove(nil)
	require.Errorf(t, err, "queue Remove() failed")
}

func TestQueueProxy_RemoveEmpty(t *testing.T) {
	defer queue.Clear()
	removed, err := queue.Remove("1")
	require.NoError(t, err)
	assert.Equalf(t, removed, false, "queue Remove() failed")
}

func TestQueueProxy_RemoveAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	removedAll, err := queue.RemoveAll([]interface{}{"2", "3"})
	require.NoError(t, err)
	assert.Equalf(t, removedAll, true, "queue RemoveAll() failed")
	found, err := queue.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, true, "queue RemoveAll() failed")
}

func TestQueueProxy_RemoveAllWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RemoveAll(nil)
	require.Errorf(t, err, "queue RemoveAll() failed")
}

func TestQueueProxy_RemoveAllEmpty(t *testing.T) {
	defer queue.Clear()
	removedAll, err := queue.RemoveAll([]interface{}{"2", "3"})
	require.NoError(t, err)
	assert.Equalf(t, removedAll, false, "queue RemoveAll() failed")
}

func TestQueueProxy_RetainAll(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	changed, err := queue.RetainAll([]interface{}{"2", "3"})
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "queue RetainAll() failed")
	found, err := queue.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "queue RetainAll() failed")
}

func TestQueueProxy_RetainAllWithNilElement(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RetainAll([]interface{}{nil, "1", "2"})
	require.Errorf(t, err, "queue RetainAll() should return error with nil element")
}

func TestQueueProxy_RetainAllWithNilSlice(t *testing.T) {
	defer queue.Clear()
	_, err := queue.RetainAll(nil)
	require.Errorf(t, err, "queue RetainAll() should return error with nil slice")
}

func TestQueueProxy_Size(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2", "3"}
	queue.AddAll(all)
	size, err := queue.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(3), "queue Size() failed")
}

func TestQueueProxy_Take(t *testing.T) {
	defer queue.Clear()
	queue.Put(testElement)
	item, err := queue.Take()
	require.NoError(t, err)
	assert.Equalf(t, item, testElement, "queue Take() failed")
}

func TestQueueProxy_ToSlice(t *testing.T) {
	defer queue.Clear()
	all := []interface{}{"1", "2"}
	queue.AddAll(all)
	res, err := queue.ToSlice()
	require.NoError(t, err)
	assert.Equalf(t, res[0], all[0], "queue ToSlice() failed")
	require.NoError(t, err)
	assert.Equalf(t, res[1], all[1], "queue ToSlice() failed")
}

func TestQueueProxy_AddItemListener_IllegalListener(t *testing.T) {
	_, err := queue.AddItemListener(5, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Queue.AddItemListener should return HazelcastIllegalArgumentError")
	}
}

func TestQueueProxy_AddItemListenerItemAddedIncludeValue(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, true)
	defer queue.RemoveItemListener(registrationID)
	require.NoError(t, err)
	queue.Put(testElement)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "queue AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), testElement, "queue AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.EventType(), int32(1), "queue AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Name(), queueName, "queue AddItemListener() failed when item is added")
}

func TestQueueProxy_AddItemItemAddedListener(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	require.NoError(t, err)
	queue.Put(testElement)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "queue AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), nil, "queue AddItemListener() failed when item is added")
}

func TestQueueProxy_AddItemListenerItemRemovedIncludeValue(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	queue.Put(testElement)
	registrationID, err := queue.AddItemListener(listener, true)
	defer queue.RemoveItemListener(registrationID)
	require.NoError(t, err)
	queue.Remove(testElement)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "queue AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), testElement, "queue AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.EventType(), int32(2), "queue AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.Name(), queueName, "queue AddItemListener() failed when item is removed")
}

func TestQueueProxy_AddItemListenerItemRemoved(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	queue.Put(testElement)
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	require.NoError(t, err)
	queue.Remove(testElement)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "queue AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), nil, "queue AddItemListener() failed when item is removed")
}

func TestQueueProxy_AddItemItemRemovedListener(t *testing.T) {
	defer queue.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := queue.AddItemListener(listener, false)
	defer queue.RemoveItemListener(registrationID)
	require.NoError(t, err)
	queue.Put(testElement)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "queue AddItemListener() failed")
	assert.Equalf(t, listener.event.Item(), nil, "queue AddItemListener() failed")
}

type itemListener struct {
	wg    *sync.WaitGroup
	event core.ItemEvent
}

func (l *itemListener) ItemAdded(event core.ItemEvent) {
	l.event = event
	l.wg.Done()
}

func (l *itemListener) ItemRemoved(event core.ItemEvent) {
	l.event = event
	l.wg.Done()
}
