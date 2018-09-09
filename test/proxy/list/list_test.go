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

package list

import (
	"log"
	"sync"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var list core.List
var client hazelcast.Instance
var testElement = "testElement"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
	list, _ = client.GetList("myList")
	m.Run()
	list.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListProxy_Add(t *testing.T) {
	defer list.Clear()
	changed, err := list.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "list Add() failed")
	result, err := list.Get(0)
	require.NoError(t, err)
	assert.Equalf(t, result, testElement, "list Add() failed")
}

func TestListProxy_AddNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.Add(nil)
	require.Errorf(t, err, "list Add() did not return an error for nil element")
}

func TestListProxy_AddAt(t *testing.T) {
	defer list.Clear()
	list.AddAt(0, testElement)
	list.AddAt(1, "test2")
	result, err := list.Get(1)
	require.NoError(t, err)
	assert.Equalf(t, result, "test2", "list AddAt() failed")
}

func TestListProxy_AddAtNilElement(t *testing.T) {
	defer list.Clear()
	err := list.AddAt(0, nil)
	require.Errorf(t, err, "list AddAt() should return error for nil element")
}

func TestListProxy_AddAll(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2"}
	added, err := list.AddAll(all)
	require.NoError(t, err)
	assert.Equalf(t, added, true, "list AddAll() failed")
	res1, err := list.Get(0)
	require.NoError(t, err)
	assert.Equalf(t, res1, "1", "list AddAll() failed")
	res2, err := list.Get(1)
	require.NoError(t, err)
	assert.Equalf(t, res2, "2", "list AddAll() failed")
}

func TestListProxy_AddAllWithNilElement(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", nil}
	_, err := list.AddAll(all)
	require.Errorf(t, err, "list AddAll() should return error for nil element")
}

func TestListProxy_AddAllNilSlice(t *testing.T) {
	defer list.Clear()
	_, err := list.AddAll(nil)
	require.Errorf(t, err, "list AddAll() should return error for nil slice")
}

func TestListProxy_AddAllAt(t *testing.T) {
	defer list.Clear()
	list.AddAt(0, "0")
	all := []interface{}{"1", "2"}
	added, err := list.AddAllAt(1, all)
	require.NoError(t, err)
	assert.Equalf(t, added, true, "list AddAllAt() failed")
	res1, err := list.Get(1)
	require.NoError(t, err)

	assert.Equalf(t, res1, "1", "list AddAllAt() failed")
	res2, err := list.Get(2)
	require.NoError(t, err)
	assert.Equalf(t, res2, "2", "list AddAllAt() failed")
}

func TestListProxy_AddAllAtWithNilElement(t *testing.T) {
	defer list.Clear()
	list.AddAt(0, "0")
	all := []interface{}{"1", nil}
	_, err := list.AddAllAt(1, all)
	require.Errorf(t, err, "list AddAllAt() should return error for nil element")
}

func TestListProxy_AddAllAtNilSlice(t *testing.T) {
	defer list.Clear()
	list.AddAt(0, "0")
	_, err := list.AddAllAt(1, nil)
	require.Errorf(t, err, "list AddAllAt() should return error for nil slice")
}

func TestListProxy_Clear(t *testing.T) {
	all := []interface{}{"1", "2"}
	list.AddAll(all)
	list.Clear()
	size, err := list.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "list Clear() should clear the list")
}

func TestListProxy_Contains(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2"}
	list.AddAll(all)
	found, err := list.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, true, "list Contains() failed")
}

func TestListProxy_ContainsWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.Contains(nil)
	require.Errorf(t, err, "list Contains() should return error for nil element")
}

func TestListProxy_ContainsAll(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2"}
	list.AddAll(all)
	found, err := list.ContainsAll(all)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "list ContainsAll() failed")
}

func TestListProxy_ContainsAllNilSlice(t *testing.T) {
	defer list.Clear()
	_, err := list.ContainsAll(nil)
	require.Errorf(t, err, "list ContainsAll() should return error for nil slice")
}

func TestListProxy_ToSlice(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2"}
	list.AddAll(all)
	res, err := list.ToSlice()
	require.NoError(t, err)
	assert.Equalf(t, res[0], all[0], "list ToSlice() failed")
	assert.Equalf(t, res[1], all[1], "list ToSlice() failed")
}

func TestListProxy_IndexOf(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2"}
	list.AddAll(all)
	index, err := list.IndexOf("1")
	require.NoError(t, err)
	assert.Equalf(t, index, int32(0), "list IndexOf() failed")
}

func TestListProxy_IndexOfWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.IndexOf(nil)
	require.Errorf(t, err, "list IndexOf() should return error with nil element")
}

func TestListProxy_IsEmpty(t *testing.T) {
	defer list.Clear()
	empty, err := list.IsEmpty()
	require.NoError(t, err)
	assert.Equalf(t, empty, true, "list IsEmpty() failed")
}

func TestListProxy_LastIndexOf(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "2"}
	list.AddAll(all)
	index, err := list.LastIndexOf("2")
	require.NoError(t, err)
	assert.Equalf(t, index, int32(2), "list LastIndexOf() failed")
}

func TestListProxy_LastIndexOfWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.LastIndexOf(nil)
	require.Errorf(t, err, "list LastIndexOf() should return error with nil element")
}

func TestListProxy_Remove(t *testing.T) {
	defer list.Clear()
	list.Add("1")
	removed, err := list.Remove("1")
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "list Remove() failed")
}

func TestListProxy_RemoveWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.Remove(nil)
	require.Errorf(t, err, "list Remove() should return error with nil element")
}

func TestListProxy_RemoveAt(t *testing.T) {
	defer list.Clear()
	list.Add("1")
	previous, err := list.RemoveAt(0)
	require.NoError(t, err)
	assert.Equalf(t, previous, "1", "list RemoveAt() failed")
}

func TestListProxy_RemoveAll(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "3"}
	list.AddAll(all)
	removedAll, err := list.RemoveAll([]interface{}{"2", "3"})
	require.NoError(t, err)
	assert.Equalf(t, removedAll, true, "list RemoveAll() failed")
	found, err := list.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, true, "list RemoveAll() failed")
}

func TestListProxy_RemoveAllWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.RemoveAll([]interface{}{nil, "1", "2"})
	require.Errorf(t, err, "list RemoveAll() should return error with nil element")
}

func TestListProxy_RemoveAllWithNilSlice(t *testing.T) {
	defer list.Clear()
	_, err := list.RemoveAll(nil)
	require.Errorf(t, err, "list RemoveAll() should return error with nil slice")
}

func TestListProxy_RetainAll(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "3"}
	list.AddAll(all)
	changed, err := list.RetainAll([]interface{}{"2", "3"})
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "list RetainAll() failed")
	found, err := list.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, found, false, "list RetainAll() failed")
}

func TestListProxy_RetainAllWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.RetainAll([]interface{}{nil, "1", "2"})
	require.Errorf(t, err, "list RetainAll() should return error with nil element")
}

func TestListProxy_RetainAllWithNilSlice(t *testing.T) {
	defer list.Clear()
	_, err := list.RetainAll(nil)
	require.Errorf(t, err, "list RetainAll() should return error with nil slice")
}

func TestListProxy_Size(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "3"}
	list.AddAll(all)
	size, err := list.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(3), "list Size() failed")
}

func TestListProxy_Set(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "3"}
	list.AddAll(all)
	list.Set(1, "13")
	res, err := list.Get(1)
	require.NoError(t, err)
	assert.Equalf(t, res, "13", "list Set() failed")
}

func TestListProxy_SetWithNilElement(t *testing.T) {
	defer list.Clear()
	_, err := list.Set(0, nil)
	require.Errorf(t, err, "list Set() should return error with nil element")
}

func TestListProxy_SubList(t *testing.T) {
	defer list.Clear()
	all := []interface{}{"1", "2", "3"}
	list.AddAll(all)
	res, err := list.SubList(1, 3)
	require.NoError(t, err)
	assert.Equalf(t, res[0], "2", "list SubList() failed")
	assert.Equalf(t, res[1], "3", "list SubList() failed")
}

func TestListProxy_SetWithoutItem(t *testing.T) {
	defer list.Clear()
	_, err := list.Set(1, "a")
	require.Errorf(t, err, "list Set() should return error with index error")
}

func TestListProxy_AddItemListener_IllegalListener(t *testing.T) {
	_, err := list.AddItemListener(5, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("List.AddItemListener should return HazelcastIllegalArgumentError")
	}
}

func TestListProxy_AddItemListenerItemAddedIncludeValue(t *testing.T) {
	defer list.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := list.AddItemListener(listener, true)
	defer list.RemoveItemListener(registrationID)
	require.NoError(t, err)
	list.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "list AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), testElement, "list AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.EventType(), int32(1), "list AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Name(), "myList", "list AddItemListener() failed when item is added")
}

func TestListProxy_AddItemItemAddedListener(t *testing.T) {
	defer list.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := list.AddItemListener(listener, false)
	defer list.RemoveItemListener(registrationID)
	require.NoError(t, err)
	list.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "list AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), nil, "list AddItemListener() failed when item is added")
}

func TestListProxy_AddItemListenerItemRemovedIncludeValue(t *testing.T) {
	defer list.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	list.Add(testElement)
	registrationID, err := list.AddItemListener(listener, true)
	defer list.RemoveItemListener(registrationID)
	require.NoError(t, err)
	list.Remove(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "list AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), testElement, "list AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.EventType(), int32(2), "list AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.Name(), "myList", "list AddItemListener() failed when item is removed")
}

func TestListProxy_AddItemListenerItemRemoved(t *testing.T) {
	defer list.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	list.Add(testElement)
	registrationID, err := list.AddItemListener(listener, false)
	defer list.RemoveItemListener(registrationID)
	require.NoError(t, err)
	list.Remove(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "list AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), nil, "list AddItemListener() failed when item is removed")
}

func TestListProxy_AddItemItemRemovedListener(t *testing.T) {
	defer list.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := list.AddItemListener(listener, false)
	defer list.RemoveItemListener(registrationID)
	require.NoError(t, err)
	list.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "list AddItemListener() failed")
	assert.Equalf(t, listener.event.Item(), nil, "list AddItemListener() failed")
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
