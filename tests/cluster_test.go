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

package tests

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	"log"
	"sync"
	"testing"
	"time"
)

type membershipListener struct {
	wg *sync.WaitGroup
}

func (membershipListener *membershipListener) MemberAdded(member core.IMember) {
	membershipListener.wg.Done()
}
func (membershipListener *membershipListener) MemberRemoved(member core.IMember) {
	membershipListener.wg.Done()
}

var remoteController *RemoteControllerClient
var cluster *Cluster

func TestMain(m *testing.M) {
	rc, err := NewRemoteControllerClient("localhost:9701")
	remoteController = rc
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}
func TestInitialMembershipListener(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(1)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster initialMembershipListener failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestMemberAddedandRemoved(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(1)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster initialMembershipListener failed")
	wg.Add(1)
	member, _ := remoteController.StartMember(cluster.ID)
	timeout = WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster memberAdded failed")
	wg.Add(1)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	timeout = WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster memberRemoved failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestAddListener(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	wg.Add(1)
	registrationId := client.GetCluster().AddListener(&membershipListener{wg: wg})
	member, _ := remoteController.StartMember(cluster.ID)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster initialMembershipListener failed")
	client.GetCluster().RemoveListener(registrationId)
	wg.Add(1)
	member2, _ := remoteController.StartMember(cluster.ID)
	timeout = WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "Cluster RemoveListener failed")
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	registrationId = client.GetCluster().AddListener(&membershipListener{wg: wg})
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	timeout = WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster memberRemoved failed")
	client.GetCluster().RemoveListener(registrationId)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestAddListeners(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	wg.Add(2)
	registrationId1 := client.GetCluster().AddListener(&membershipListener{wg: wg})
	registrationId2 := client.GetCluster().AddListener(&membershipListener{wg: wg})
	remoteController.StartMember(cluster.ID)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster initialMembershipListener failed")
	client.GetCluster().RemoveListener(registrationId1)
	client.GetCluster().RemoveListener(registrationId2)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestGetMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	member2, _ := remoteController.StartMember(cluster.ID)
	member3, _ := remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	members := client.GetCluster().GetMemberList()
	AssertEqualf(t, nil, len(members), 3, "GetMemberList returned wrong number of members")
	for _, member := range members {
		AssertEqualf(t, nil, member.IsLiteMember(), false, "member shouldnt be a lite member")
		AssertEqualf(t, nil, len(member.Attributes()), 0, "member shouldnt have any attributes")
	}
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	remoteController.ShutdownMember(cluster.ID, member3.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}
func TestGetMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	member2, _ := remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	address := protocol.NewAddressWithParameters(member1.GetHost(), int(member1.GetPort()))
	member := client.GetCluster().GetMember(address)
	AssertEqualf(t, nil, member.Uuid(), member1.GetUUID(), "GetMember returned wrong member")
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}
func TestGetInvalidMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	address := protocol.NewAddressWithParameters(member1.GetHost(), 0)
	member := client.GetCluster().GetMember(address)
	AssertEqualf(t, nil, member, nil, "GetMember should have returned nil")
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}
func TestAuthenticationWithWrongCredentials(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.GroupConfig().SetName("wrongName")
	config.GroupConfig().SetPassword("wrongPassword")
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if _, ok := err.(*core.HazelcastAuthenticationError); !ok {
		t.Fatal("client should have returned an authentication error")
	}
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestClientWithoutMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	client, err := hazelcast.NewHazelcastClient()
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Fatal("client should have returned a hazelcastError")
	}
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestRestartMember(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	wg.Add(1)
	registratonId := client.(*internal.HazelcastClient).LifecycleService.AddListener(&lifecycleListener)
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "clusterService reconnect has failed")
	AssertEqualf(t, nil, lifecycleListener.collector[0], internal.LIFECYCLE_STATE_DISCONNECTED, "clusterService reconnect has failed")
	wg.Add(1)
	remoteController.StartMember(cluster.ID)
	timeout = WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "clusterService reconnect has failed")
	AssertEqualf(t, nil, lifecycleListener.collector[1], internal.LIFECYCLE_STATE_CONNECTED, "clusterService reconnect has failed")
	client.GetLifecycle().RemoveListener(&registratonId)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestReconnectToNewNodeViaLastMemberList(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	oldMember, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(100)
	config.ClientNetworkConfig().SetSmartRouting(false)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	newMember, _ := remoteController.StartMember(cluster.ID)
	remoteController.ShutdownMember(cluster.ID, oldMember.UUID)
	time.Sleep(10 * time.Second)
	memberList := client.GetCluster().GetMemberList()
	AssertEqualf(t, nil, len(memberList), 1, "client did not use the last member list to reconnect")
	AssertEqualf(t, nil, memberList[0].Uuid(), newMember.UUID, "client did not use the last member list to reconnect uuid")
	remoteController.ShutdownCluster(cluster.ID)
	client.Shutdown()
}
func TestConnectToClusterWithoutPort(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().AddAddress("127.0.0.1")
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	members := client.GetCluster().GetMemberList()
	AssertEqualf(t, nil, members[0].Address().Host(), "localhost", "connectToClusterWithoutPort returned a wrong member address")
	AssertEqualf(t, nil, len(members), 1, "connectToClusterWithoutPort returned a wrong member address")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

type mapListener struct {
	wg *sync.WaitGroup
}

func (ml *mapListener) EntryAdded(event core.IEntryEvent) {
	ml.wg.Done()
}
