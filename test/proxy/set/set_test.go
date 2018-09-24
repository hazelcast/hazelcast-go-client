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

package set

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

var set core.Set
var client hazelcast.Client
var testElement = "testElement"

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
	set, _ = client.GetSet("mySet")
	m.Run()
	set.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestSetProxy_Add(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	found, err := set.Contains(testElement)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "set Add() failed")
}

func TestSetProxy_AddNil(t *testing.T) {
	defer set.Clear()
	_, err := set.Add(nil)
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_AddAll(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2"}
	added, err := set.AddAll(all)
	require.NoError(t, err)
	assert.Equalf(t, added, true, "set AddAll() failed")
	res1, err := set.Contains("1")
	require.NoError(t, err)
	assert.Equalf(t, res1, true, "set AddAll() failed")
	res2, err := set.Contains("2")
	require.NoError(t, err)
	assert.Equalf(t, res2, true, "set AddAll() failed")
}

func TestSetProxy_AddAllWithNilElement(t *testing.T) {
	defer set.Clear()
	all := []interface{}{nil, "2"}
	_, err := set.AddAll(all)
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_AddAllWithNilSlice(t *testing.T) {
	defer set.Clear()
	_, err := set.AddAll(nil)
	require.Errorf(t, err, "nil slice should return an error.")
}

func TestSetProxy_Clear(t *testing.T) {
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	set.Clear()
	size, err := set.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(0), "set Clear() failed")
}

func TestSetProxy_Contains(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Contains() failed")
	found, err := set.Contains(testElement)
	require.NoError(t, err)
	assert.Equalf(t, found, true, "set Contains() failed")
}

func TestSetProxy_ContainsNil(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	_, err = set.Contains(nil)
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_ContainsAll(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2"}
	set.AddAll(all)
	foundAll, err := set.ContainsAll(all)
	require.NoError(t, err)
	assert.Equalf(t, foundAll, true, "set ContainsAll() failed")
}

func TestSetProxy_ContainsAllNilElement(t *testing.T) {
	defer set.Clear()
	all := []interface{}{nil, "2"}
	set.AddAll(all)
	_, err := set.ContainsAll(all)
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_ContainsAllNilSlice(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2"}
	set.AddAll(all)
	_, err := set.ContainsAll(nil)
	require.Errorf(t, err, "nil slice should return an error.")
}

func TestSetProxy_ToSlice(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2"}
	set.AddAll(all)
	items, err := set.ToSlice()
	require.NoError(t, err)
	assert.Equalf(t, items[0], "2", "set GetAll() failed")
	require.NoError(t, err)
	assert.Equalf(t, items[1], "1", "set GetAll() failed")
}

func TestSetProxy_IsEmpty(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2"}
	set.AddAll(all)
	isEmpty, err := set.IsEmpty()
	require.NoError(t, err)
	assert.Equalf(t, isEmpty, false, "set IsEmpty() failed")
}

func TestSetProxy_Remove(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	removed, err := set.Remove(testElement)
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "set Remove() failed")
}

func TestSetProxy_RemoveNil(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	_, err = set.Remove(nil)
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_RemoveAll(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	removed, err := set.RemoveAll([]interface{}{"1", "2"})
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "set RemoveAll() failed")
	items, err := set.ToSlice()
	require.NoError(t, err)
	assert.Equalf(t, items[0], "3", "set RemoveAll() failed")
}

func TestSetProxy_RemoveAllNilElement(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	_, err := set.RemoveAll([]interface{}{nil, "2"})
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_RemoveAllNilSlice(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	_, err := set.RemoveAll(nil)
	require.Errorf(t, err, "nil slice should return an error")
}

func TestSetProxy_RetainAll(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	changed, err := set.RetainAll([]interface{}{"2"})
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set RetainAll() failed")
	items, err := set.ToSlice()
	require.NoError(t, err)
	assert.Equalf(t, items[0], "2", "set RetainAll() failed")
}

func TestSetProxy_RetainAllNilElement(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	_, err := set.RetainAll([]interface{}{nil})
	require.Errorf(t, err, "nil element should return an error.")
}

func TestSetProxy_RetainAllNilSlice(t *testing.T) {
	defer set.Clear()
	all := []interface{}{"1", "2", "3"}
	set.AddAll(all)
	_, err := set.RetainAll(nil)
	require.Errorf(t, err, "nil slice should return an error")
}

func TestSetProxy_Size(t *testing.T) {
	defer set.Clear()
	changed, err := set.Add(testElement)
	require.NoError(t, err)
	assert.Equalf(t, changed, true, "set Add() failed")
	size, err := set.Size()
	require.NoError(t, err)
	assert.Equalf(t, size, int32(1), "set Size() failed")
}

func TestSetProxy_AddItemListener_IllegalListener(t *testing.T) {
	_, err := set.AddItemListener(5, true)
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Error("Set.AddItemListener should return HazelcastIllegalArgumentError")
	}
}

func TestSetProxy_AddItemListenerItemAddedIncludeValue(t *testing.T) {
	defer set.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := set.AddItemListener(listener, true)
	defer set.RemoveItemListener(registrationID)
	require.NoError(t, err)
	set.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "set AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), testElement, "set AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.EventType(), int32(1), "set AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Name(), "mySet", "set AddItemListener() failed when item is added")
}

func TestSetProxy_AddItemItemAddedListener(t *testing.T) {
	defer set.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := set.AddItemListener(listener, false)
	defer set.RemoveItemListener(registrationID)
	require.NoError(t, err)
	set.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "set AddItemListener() failed when item is added")
	assert.Equalf(t, listener.event.Item(), nil, "set AddItemListener() failed when item is added")
}

func TestSetProxy_AddItemListenerItemRemovedIncludeValue(t *testing.T) {
	defer set.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	set.Add(testElement)
	registrationID, err := set.AddItemListener(listener, true)
	defer set.RemoveItemListener(registrationID)
	require.NoError(t, err)
	set.Remove(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "set AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), testElement, "set AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.EventType(), int32(2), "set AddItemListener() failed when item is removed")
	assert.Equalf(t, listener.event.Name(), "mySet", "set AddItemListener() failed when item is removed")
}

func TestSetProxy_AddItemListenerItemRemoved(t *testing.T) {
	defer set.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	set.Add(testElement)
	registrationID, err := set.AddItemListener(listener, false)
	defer set.RemoveItemListener(registrationID)
	require.NoError(t, err)
	set.Remove(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "set AddItemListenerItemRemoved() failed when item is removed")
	assert.Equalf(t, listener.event.Item(), nil, "set AddItemListener() failed when item is removed")
}

func TestSetProxy_AddItemItemRemovedListener(t *testing.T) {
	defer set.Clear()
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &itemListener{wg: wg}
	registrationID, err := set.AddItemListener(listener, false)
	defer set.RemoveItemListener(registrationID)
	require.NoError(t, err)
	set.Add(testElement)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equalf(t, false, timeout, "set AddItemListener() failed")
	assert.Equalf(t, listener.event.Item(), nil, "set AddItemListener() failed")
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
