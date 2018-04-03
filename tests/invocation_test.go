// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless re®quired by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

func TestNonSmartInvoke(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetSmartRouting(false)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("myMap")
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	assert.Equalf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
	remoteController.ShutdownCluster(cluster.ID)
	client.Shutdown()
}

func TestSingleConnectionWithManyMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetSmartRouting(false)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("testMap")
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
		res, err := mp.Get(testKey)
		assert.Equalf(t, err, res, testValue, "get returned a wrong value")
	}
	mp.Clear()
	connectionCount := client.(*internal.HazelcastClient).ConnectionManager.ConnectionCount()
	assert.Equalf(t, nil, int32(1), connectionCount, "Client should open only one connection")
	remoteController.ShutdownCluster(cluster.ID)
	client.Shutdown()
}

func TestInvocationTimeout(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetRedoOperation(true)
	config.ClientNetworkConfig().SetConnectionAttemptLimit(100)
	config.ClientNetworkConfig().SetInvocationTimeout(5 * time.Second)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("testMap")
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	_, err := mp.Put("a", "b")
	if _, ok := err.(*core.HazelcastTimeoutError); !ok {
		t.Fatal("invocation should have timed out but returned, ", err)
	}
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestInvocationRetry(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetRedoOperation(true)
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("testMap")
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	mu := sync.Mutex{}
	//Open the new member in a new subroutine after 5 seconds to ensure that Put will be forced to retry.
	go func() {
		time.Sleep(5 * time.Second)
		mu.Lock()
		remoteController.StartMember(cluster.ID)
		mu.Unlock()
	}()
	_, err := mp.Put("testKey", "testValue")
	assert.Nilf(t, err, nil, "InvocationRetry failed")
	result, err := mp.Get("testKey")
	assert.Equalf(t, err, result, "testValue", "invocation retry failed")
	client.Shutdown()
	mu.Lock()
	remoteController.ShutdownCluster(cluster.ID)
	mu.Unlock()
}

func TestInvocationWithShutdown(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetRedoOperation(true)
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("testMap")
	client.Shutdown()
	_, err := mp.Put("testingKey", "testingValue")
	if _, ok := err.(*core.HazelcastClientNotActiveError); !ok {
		t.Fatal("HazelcastClientNotActiveError was expected")
	}
	remoteController.ShutdownCluster(cluster.ID)
}

func TestInvocationNotSent(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	member, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetRedoOperation(true)
	config.ClientNetworkConfig().SetConnectionAttemptLimit(100)
	config.ClientNetworkConfig().SetInvocationTimeout(10 * time.Second)
	config.ClientNetworkConfig().SetConnectionAttemptPeriod(1 * time.Second)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mp, _ := client.GetMap("testMap")
	wg.Add(50)
	// make put ops concurrently so that some of them will not be sent when the server gets shut down.
	go func() {
		for i := 0; i < 50; i++ {
			go func() {
				_, err := mp.Put("testKey", "testValue")
				wg.Done()
				if err != nil {
					t.Fatal("put should have been retried, the error was :", err)
				}
			}()

		}
	}()
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	time.Sleep(3 * time.Second)
	remoteController.StartMember(cluster.ID)
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, nil, timeout, false, "invocationNotSent failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)

}
