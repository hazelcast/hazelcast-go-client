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

package test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestListenerWhenNodeLeftAndReconnected(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationID, err := mp.AddEntryListener(entryAdded, true)
	require.NoError(t, err)
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	time.Sleep(3 * time.Second)
	remoteController.StartMember(cluster.ID)
	time.Sleep(4 * time.Second)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, false, timeout, "listener reregister failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListenerWithMultipleMembers(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationID, err := mp.AddEntryListener(entryAdded, true)
	require.NoError(t, err)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, false, timeout, "smartListener with multiple members failed")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListenerWithMemberConnectedAfterAWhile(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationID, err := mp.AddEntryListener(entryAdded, true)
	require.NoError(t, err)
	remoteController.StartMember(cluster.ID)
	time.Sleep(15 * time.Second) // Wait for partitionTable update
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, false, timeout, "smartListener adding a member after a while failed to listen.")
	mp.RemoveEntryListener(registrationID)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
