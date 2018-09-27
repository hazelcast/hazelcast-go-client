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

	"math/rand"
	"sync"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"

	"github.com/stretchr/testify/assert"
)

func TestClientGetMapWhenNoMemberUp(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	remoteController.ShutdownCluster(cluster.ID)
	_, err := client.GetMap("map")
	assert.Errorf(t, err, "getMap should have returned an error when no member is up")
	client.Shutdown()
}

func TestClientShutdownAndReopen(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
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
	assert.NoError(t, err)
	assert.Equalf(t, value, "value", "Client shutdown and reopen failed")
	client.Shutdown()
}

func TestClientRoutineLeakage(t *testing.T) {
	cluster, err := remoteController.CreateCluster("", DefaultServerConfig)
	if err != nil {
		t.Fatal(err)
	}
	_, err = remoteController.StartMember(cluster.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(cluster.ID)
	time.Sleep(2 * time.Second)
	routineNumBefore := runtime.NumGoroutine()
	client, _ := hazelcast.NewClient()
	testMp, _ := client.GetMap("test")
	testMp.Put("key", "value")
	client.Shutdown()
	time.Sleep(4 * time.Second)
	routineNumAfter := runtime.NumGoroutine()
	assert.Equal(t, routineNumBefore, routineNumAfter)
}

func TestConnectionTimeout(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	cfg := hazelcast.NewConfig()
	cfg.NetworkConfig().SetConnectionTimeout(0)
	client, err := hazelcast.NewClient()
	defer client.Shutdown()
	assert.NoError(t, err)
}

func TestNegativeConnectionTimeoutShouldPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Negative connection timeout count should panic.")
		}
	}()
	cfg := hazelcast.NewConfig()
	cfg.NetworkConfig().SetConnectionTimeout(-5 * time.Second)
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
	assert.Equal(t, connectionManager.NextConnectionID(), int64(6))

	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestClientNameSet(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.SetClientName("client1")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	assert.Equal(t, client.Name(), "client1")
}

func TestClientNameDefault(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	defer client.Shutdown()
	assert.Contains(t, client.Name(), "hz.client_")
}

func TestMultipleClientNameDefault(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	names := make(map[string]struct{})
	for i := 1; i <= 10; i++ {
		client, _ := hazelcast.NewClient()
		defer client.Shutdown()
		names[client.Name()] = struct{}{}
	}
	assert.Len(t, names, 10)
}

func TestMultipleClientNameDefaultConcurrent(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	mu := sync.Mutex{}
	names := make(map[string]struct{})
	for i := 1; i <= 10; i++ {
		go func() {
			client, _ := hazelcast.NewClient()
			defer client.Shutdown()
			mu.Lock()
			names[client.Name()] = struct{}{}
			mu.Unlock()
		}()
	}
	time.Sleep(time.Second)
	mu.Lock()
	assert.Len(t, names, 10)
	mu.Unlock()

}

func TestGetDistributedObjectWithNotRegisteredServiceName(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, err := hazelcast.NewClient()
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

func TestGetDistributedObjectsWhenClientNotActive(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	remoteController.ShutdownCluster(cluster.ID)
	name := "test"
	message := "Distributed object should not be created when client is not active"
	_, err := client.GetMap(name)
	assert.Errorf(t, err, message)

	_, err = client.GetTopic(name)
	assert.Errorf(t, err, message)

	_, err = client.GetReplicatedMap(name)
	assert.Errorf(t, err, message)

	_, err = client.GetSet(name)
	assert.Errorf(t, err, message)

	_, err = client.GetQueue(name)
	assert.Errorf(t, err, message)

	_, err = client.GetList(name)
	assert.Errorf(t, err, message)

	_, err = client.GetMultiMap(name)
	assert.Errorf(t, err, message)

	_, err = client.GetPNCounter(name)
	assert.Errorf(t, err, message)

	_, err = client.GetRingbuffer(name)
	assert.Errorf(t, err, message)

	_, err = client.GetFlakeIDGenerator(name)
	assert.Errorf(t, err, message)

}

func TestHazelcastError_ServerError(t *testing.T) {
	crdtReplicationDelayedConfig, _ := Read("./proxy/pncounter/crdt_replication_delayed_config.xml")
	cluster, err := remoteController.CreateCluster("", crdtReplicationDelayedConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	cfg := hazelcast.NewConfig()
	cfg.SetProperty(property.InvocationTimeoutSeconds.Name(), "5")
	client, _ := hazelcast.NewClientWithConfig(cfg)
	counter, _ := client.GetPNCounter("myPNCounter")
	var delta int64 = 5
	counter.GetAndAdd(delta)
	target := client.(*internal.HazelcastClient).ClusterService.GetMember(internal.GetCurrentTargetReplicaAddress(counter))
	remoteController.TerminateMember(cluster.ID, target.UUID())
	_, err = counter.Get()
	if _, ok := err.(*core.HazelcastConsistencyLostError); !ok {
		t.Fatal("PNCounter.Get should return HazelcastConsistencyLostError")
	}

	cErr, _ := err.(*core.HazelcastConsistencyLostError)
	if cErr.ServerError() == nil {
		t.Fatal("Server error should not be nil")
	}
	serverErr := cErr.ServerError()
	if len(serverErr.Message()) == 0 {
		t.Error("serverError message should be longer than 0.")
	}

	if len(serverErr.ClassName()) == 0 {
		t.Error("serverError ClassName should be longer than 0.")
	}

	if len(serverErr.StackTrace()) == 0 {
		t.Error("serverError StackTrace should be longer than 0.")
	}

}
