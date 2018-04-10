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

package pn_counter

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"sync"
	"testing"
)

var counter core.PNCounter
var client hazelcast.IHazelcastInstance

const counterName = "myPNCounter"

var remoteController *RemoteControllerClient
var cluster *Cluster
var err error

func TestMain(m *testing.M) {
	remoteController, err = NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	counter, _ = client.GetPNCounter(counterName)
	m.Run()
	counter.Destroy()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func destroyAndCreate() {
	counter.Destroy()
	counter, _ = client.GetPNCounter(counterName)
}

func TestPNCounter_Name(t *testing.T) {
	if counterName != counter.Name() {
		t.Errorf("PNCounter.Name failed")
	}
}

func TestPNCounter_ServiceName(t *testing.T) {
	serviceName := common.SERVICE_NAME_PN_COUNTER
	if serviceName != counter.ServiceName() {
		t.Errorf("PNCounter.ServiceName failed")
	}
}

func TestPNCounter_PartitionKey(t *testing.T) {
	if counterName != counter.PartitionKey() {
		t.Errorf("PNCounter.PartitionKey failed")
	}
}

func TestPNCounter_Destroy(t *testing.T) {
	var delta int64 = 5
	counter.AddAndGet(delta)
	counter.Destroy()
	counter, _ = client.GetPNCounter(counterName)
	res, err := counter.Get()
	AssertEqualf(t, err, res, int64(0), "PNCounter.Destroy failed")
}

func TestPNCounter_Get(t *testing.T) {
	defer destroyAndCreate()
	var delta int64 = 5
	counter.AddAndGet(delta)
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, delta, "PNCounter.Get failed")
}

func TestPNCounter_GetAndAdd(t *testing.T) {
	defer destroyAndCreate()
	var delta int64 = 5
	previousValue, err := counter.GetAndAdd(delta)
	AssertEqualf(t, err, previousValue, int64(0), "PNCounter.GetAndAdd failed")
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, delta, "PNCounter.GetAndAdd failed")
}

func TestPNCounter_AddAndGet(t *testing.T) {
	defer destroyAndCreate()
	var delta int64 = 5
	updatedValue, err := counter.AddAndGet(delta)
	AssertEqualf(t, err, updatedValue, delta, "PNCounter.AddAndGet failed")
}

func TestPNCounter_GetAndSubtract(t *testing.T) {
	defer destroyAndCreate()
	var delta int64 = 5
	previousValue, err := counter.GetAndSubtract(delta)
	AssertEqualf(t, err, previousValue, int64(0), "PNCounter.GetAndSubtract failed")
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, -delta, "PNCounter.GetAndAddSubtract failed")
}

func TestPNCounter_SubtractAndGet(t *testing.T) {
	defer destroyAndCreate()
	var delta int64 = 5
	updatedValue, err := counter.SubtractAndGet(delta)
	AssertEqualf(t, err, updatedValue, -delta, "PNCounter.SubtractAndGet failed")
}

func TestPNCounter_DecrementAndGet(t *testing.T) {
	defer destroyAndCreate()
	updatedValue, err := counter.DecrementAndGet()
	AssertEqualf(t, err, updatedValue, int64(-1), "PNCounter.DecrementAndGet failed")
}

func TestPNCounter_IncrementAndGet(t *testing.T) {
	defer destroyAndCreate()
	updatedValue, err := counter.IncrementAndGet()
	AssertEqualf(t, err, updatedValue, int64(1), "PNCounter.IncrementAndGet failed")
}

func TestPNCounter_GetAndDecrement(t *testing.T) {
	defer destroyAndCreate()
	previousValue, err := counter.GetAndDecrement()
	AssertEqualf(t, err, previousValue, int64(0), "PNCounter.GetAndDecrement failed")
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, int64(-1), "PNCounter.GetAndDecrement failed")

}

func TestPNCounter_GetAndIncrement(t *testing.T) {
	defer destroyAndCreate()
	previousValue, err := counter.GetAndIncrement()
	AssertEqualf(t, err, previousValue, int64(0), "PNCounter.GetAndIncrement failed")
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, int64(1), "PNCounter.GetAndIncrement failed")
}

func TestPNCounter_ManyAdd(t *testing.T) {
	defer destroyAndCreate()
	var wg sync.WaitGroup
	var delta = 1000
	wg.Add(delta)
	for i := 0; i < delta; i++ {
		go func() {
			counter.IncrementAndGet()
			wg.Done()
		}()
	}
	wg.Wait()
	currentValue, err := counter.Get()
	AssertEqualf(t, err, currentValue, int64(delta), "PNCounter has race condition")

}

func TestPNCounter_HazelcastNoDataMemberInClusterError(t *testing.T) {
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
	liteMemberConfig, _ := Read("lite_member_config.xml")
	cluster, err = remoteController.CreateCluster("", liteMemberConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	counter, _ = client.GetPNCounter(counterName)
	var delta int64 = 5
	_, err = counter.AddAndGet(delta)
	if _, ok := err.(*core.HazelcastNoDataMemberInClusterError); !ok {
		t.Errorf("PNCounter.AddAndGet should return HazelcastNoDataMemberInClusterError")
	}
}

func TestPNCounter_HazelcastConsistencyLostError(t *testing.T) {
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
	crdtReplicationDelayedConfig, _ := Read("crdt_replication_delayed_config.xml")
	cluster, err = remoteController.CreateCluster("", crdtReplicationDelayedConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	counter, _ = client.GetPNCounter(counterName)
	var delta int64 = 5
	counter.GetAndAdd(delta)
	target := client.GetCluster().GetMember(counter.(*internal.PNCounterProxy).GetCurrentTargetReplicaAddress())
	remoteController.TerminateMember(cluster.ID, target.Uuid())
	_, err = counter.Get()
	if _, ok := err.(*core.HazelcastConsistencyLostError); !ok {
		t.Errorf("PNCounter.Get should return HazelcastConsistencyLostError")
	}
}

func TestPNCounter_Reset(t *testing.T) {
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
	crdtReplicationDelayedConfig, _ := Read("crdt_replication_delayed_config.xml")
	cluster, err = remoteController.CreateCluster("", crdtReplicationDelayedConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	counter, _ = client.GetPNCounter(counterName)
	var delta int64 = 5
	counter.GetAndAdd(delta)
	target := client.GetCluster().GetMember(counter.(*internal.PNCounterProxy).GetCurrentTargetReplicaAddress())
	remoteController.TerminateMember(cluster.ID, target.Uuid())
	counter.Reset()
	currentValue, err := counter.AddAndGet(delta)
	AssertEqualf(t, err, currentValue, int64(delta), "PNCounter.Reset failed")
}
