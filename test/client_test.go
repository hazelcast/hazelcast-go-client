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
	"testing"

	"time"

	"runtime"

	"log"

	"math/rand"
	"sync"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

func TestClientGetMapWhenNoMemberUp(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	remoteController.ShutdownCluster(cluster.ID)
	_, err := client.GetMap("map")
	assert.ErrorNotNil(t, err, "getMap should have returned an error when no member is up")
	client.Shutdown()
}

func TestClientShutdownAndReopen(t *testing.T) {
	cluster, err := remoteController.CreateCluster("", DefaultServerConfig)
	log.Println(err, cluster)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	testMp, _ := client.GetMap("test")
	testMp.Put("key", "value")
	client.Shutdown()
	time.Sleep(2 * time.Second)

	client, _ = hazelcast.NewClient()
	testMp, _ = client.GetMap("test")
	value, err := testMp.Get("key")
	assert.Equalf(t, err, value, "value", "Client shutdown and reopen failed")
	client.Shutdown()
}

func TestClientRoutineLeakage(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)
	time.Sleep(2 * time.Second)
	routineNumBefore := runtime.NumGoroutine()
	client, _ := hazelcast.NewClient()
	testMp, _ := client.GetMap("test")
	testMp.Put("key", "value")
	client.Shutdown()
	time.Sleep(4 * time.Second)
	routineNumAfter := runtime.NumGoroutine()
	if routineNumBefore != routineNumAfter {
		t.Fatalf("Expected number of routines %d, found %d", routineNumBefore, routineNumAfter)
	}
}

func TestOpenedClientConnectionCount_WhenMultipleMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	for i := 0; i < 5; i++ {
		remoteController.StartMember(cluster.ID)
	}
	client, _ := hazelcast.NewClient()

	m, _ := client.GetMap("test")
	var waitGroup sync.WaitGroup
	waitGroup.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				m.Put(rand.Int(), rand.Int())
			}
			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
	connectionManager := client.(*internal.HazelcastClient).ConnectionManager
	//There should be 5 connections. Next id will be 6
	assert.Equal(t, nil, connectionManager.NextConnectionID(), int64(6))

	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestGetDistributedObjectWithNotRegisteredServiceName(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	clientConfig := hazelcast.NewConfig()
	clientConfig.NetworkConfig().AddAddress("127.0.0.1:5701")
	client, err := hazelcast.NewClientWithConfig(clientConfig)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
	serviceName := "InvalidServiceName"
	_, err = client.GetDistributedObject(serviceName, "testName")
	if _, ok := err.(*core.HazelcastClientServiceNotFoundError); ok {
		t.Error("HazelcastClientServiceNotFoundError expected got :", err)
	}

}
