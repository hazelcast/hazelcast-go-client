// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package client

import (
	"log"
	"sync"
	"testing"

	"sync/atomic"

	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/config"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/stretchr/testify/assert"
)

type membershipListener struct {
	wg    *sync.WaitGroup
	event atomic.Value
}

func (l *membershipListener) MemberAttributeChanged(event core.MemberAttributeEvent) {
	l.wg.Done()
	l.event.Store(event)
}

func (l *membershipListener) MemberAdded(member core.Member) {
	l.wg.Done()
}

func (l *membershipListener) MemberRemoved(member core.Member) {
	l.wg.Done()
}

var remoteController *rc.RemoteControllerClient
var cluster *rc.Cluster

func TestMain(m *testing.M) {
	rc, err := rc.NewRemoteControllerClient("localhost:9701")
	remoteController = rc
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func TestInitialMembershipListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	config := hazelcast.NewConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(1)
	_, shutdownFunc := testutil.CreateClientAndClusterWithConfig(remoteController, config)
	defer shutdownFunc()
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster initialMembershipListener failed")
}

func TestMemberAddedAndRemoved(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(1)
	client, _ := hazelcast.NewClientWithConfig(config)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster initialMembershipListener failed")
	wg.Add(1)
	member, _ := remoteController.StartMember(cluster.ID)
	timeout = testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster memberAdded failed")
	wg.Add(1)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	timeout = testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster memberRemoved failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestAddMembershipListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	wg.Add(1)
	registrationID := client.Cluster().AddMembershipListener(&membershipListener{wg: wg})
	member, _ := remoteController.StartMember(cluster.ID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster initialMembershipListener failed")
	client.Cluster().RemoveMembershipListener(registrationID)
	wg.Add(1)
	member2, _ := remoteController.StartMember(cluster.ID)
	timeout = testutil.WaitTimeout(wg, testutil.TimeoutShort)
	assert.Equalf(t, true, timeout, "Cluster RemoveMembershipListener failed")
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	registrationID = client.Cluster().AddMembershipListener(&membershipListener{wg: wg})
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	timeout = testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster memberRemoved failed")
	client.Cluster().RemoveMembershipListener(registrationID)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestAddMembershipListenerMemberAttributeChanged(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	member, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	listener := &membershipListener{wg: wg}
	config.AddMembershipListener(listener)
	wg.Add(2) // 1 for initial member and 1 for attribute change
	client, _ := hazelcast.NewClientWithConfig(config)
	script := "function attrs() { " +
		" return instance_0.getCluster().getLocalMember().setIntAttribute(\"test\", 123); }; result=attrs();"
	res, err := remoteController.ExecuteOnController(cluster.ID, script, rc.Lang_JAVASCRIPT)
	assert.NoError(t, err)
	assert.True(t, res.Success)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.False(t, timeout)
	event := listener.event.Load().(core.MemberAttributeEvent)
	assert.Equal(t, event.Key(), "test")
	assert.Equal(t, event.Value(), "123")
	assert.Equal(t, event.Member().UUID(), member.UUID)
	assert.Equal(t, event.OperationType(), core.MemberAttributeOperationTypePut)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestAddMembershipListeners(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	wg.Add(2)
	registrationID1 := client.Cluster().AddMembershipListener(&membershipListener{wg: wg})
	registrationID2 := client.Cluster().AddMembershipListener(&membershipListener{wg: wg})
	remoteController.StartMember(cluster.ID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "Cluster initialMembershipListener failed")
	client.Cluster().RemoveMembershipListener(registrationID1)
	client.Cluster().RemoveMembershipListener(registrationID2)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestGetMembers(t *testing.T) {
	memberAmount := 2
	client, shutdownFunc := testutil.CreateClientAndClusterWithMembers(remoteController, memberAmount)
	defer shutdownFunc()

	members := client.Cluster().GetMembers()
	assert.Equalf(t, len(members), memberAmount, "GetMembers returned wrong number of members")
	for _, member := range members {
		assert.Equalf(t, member.IsLiteMember(), false, "member shouldnt be a lite member")
		assert.Equalf(t, len(member.Attributes()), 0, "member shouldnt have any attributes")
	}
}

func TestGetMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	member2, _ := remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	address := proto.NewAddressWithParameters(member1.GetHost(), int(member1.GetPort()))
	member := client.(*internal.HazelcastClient).ClusterService.GetMember(address)
	assert.Equalf(t, member.UUID(), member1.GetUUID(), "GetMember returned wrong member")
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}

func TestGetInvalidMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewClient()
	address := proto.NewAddressWithParameters(member1.GetHost(), 0)
	member := client.(*internal.HazelcastClient).ClusterService.GetMember(address)
	assert.Equalf(t, member, nil, "GetMember should have returned nil")
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}

func TestAuthenticationWithWrongCredentials(t *testing.T) {
	shutdownFunc := testutil.CreateCluster(remoteController)
	defer shutdownFunc()
	config := hazelcast.NewConfig()
	config.GroupConfig().SetName("wrongName")
	config.GroupConfig().SetPassword("wrongPassword")
	client, err := hazelcast.NewClientWithConfig(config)
	assert.Error(t, err)
	client.Shutdown()
}

func TestClientWithoutMember(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	client, err := hazelcast.NewClient()
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Fatal("client should have returned a hazelcastError")
	}
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestRestartMember(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewClientWithConfig(config)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	wg.Add(1)
	registrationID := client.LifecycleService().AddLifecycleListener(&lifecycleListener)
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "clusterService reconnect has failed")
	assert.Equalf(t, lifecycleListener.collector[0], core.LifecycleStateDisconnected, "clusterService reconnect has failed")
	wg.Add(1)
	remoteController.StartMember(cluster.ID)
	timeout = testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "clusterService reconnect has failed")
	assert.Equalf(t, lifecycleListener.collector[1], core.LifecycleStateConnected, "clusterService reconnect has failed")
	client.LifecycleService().RemoveLifecycleListener(registrationID)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestReconnectToNewNodeViaLastMemberList(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	oldMember, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(100)
	config.NetworkConfig().SetSmartRouting(false)
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(3) // 2 for initial members, 1 for leaving member
	client, _ := hazelcast.NewClientWithConfig(config)
	newMember, _ := remoteController.StartMember(cluster.ID)
	remoteController.ShutdownMember(cluster.ID, oldMember.UUID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.False(t, timeout)
	memberList := client.Cluster().GetMembers()
	assert.Equalf(t, len(memberList), 1, "client did not use the last member list to reconnect")
	assert.Equalf(t, memberList[0].UUID(), newMember.UUID, "client did not use the last member list to reconnect uuid")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestClusterScaleDown(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	var wg = new(sync.WaitGroup)
	wg.Add(3) // 2 for 2 members, 1 for leaving member
	config := hazelcast.NewConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	client, _ := hazelcast.NewClientWithConfig(config)
	assert.Len(t, client.Cluster().GetMembers(), 2)

	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.False(t, timeout)
	assert.Len(t, client.Cluster().GetMembers(), 1)
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)

}

func TestClientShouldConnectToClusterWhenValidAddresses(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)

	testCases := [...]struct {
		address string
		err     bool
	}{
		{"localhost:5701", false},
		{"localhost", false},
		{"127.0.0.1", false},
		{"127.0.0.1:5701", false},
		{"1.12.33.5:5701", true},
		{"www.invalid.com:5701", true},
	}

	for _, tc := range testCases {
		config := hazelcast.NewConfig()
		config.NetworkConfig().SetConnectionTimeout(10 * time.Millisecond)
		config.NetworkConfig().AddAddress(tc.address)
		client, err := hazelcast.NewClientWithConfig(config)
		if tc.err {
			assert.Error(t, err, tc.address)
		} else {
			assert.NoError(t, err, tc.address)
		}
		client.Shutdown()
	}
	remoteController.ShutdownCluster(cluster.ID)
}

func TestConnectToClusterWithoutPort(t *testing.T) {
	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1")
	client, shutdownFunc := testutil.CreateClientAndClusterWithConfig(remoteController, config)
	defer shutdownFunc()
	members := client.Cluster().GetMembers()
	assert.Equalf(t, members[0].Address().Host(), "127.0.0.1", "connectToClusterWithoutPort returned a wrong member address")
	assert.Equalf(t, len(members), 1, "connectToClusterWithoutPort returned a wrong member address")
}

func TestConnectToClusterWithSetAddress(t *testing.T) {
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetAddresses([]string{"127.0.0.1"})
	client, shutdownFunc := testutil.CreateClientAndClusterWithConfig(remoteController, config)
	defer shutdownFunc()

	members := client.Cluster().GetMembers()
	assert.Equalf(t, len(members), 1, "connectToClusterWithoutPort returned a wrong member address")
	assert.Equalf(t, members[0].Address().Host(), "127.0.0.1", "connectToClusterWithoutPort returned a wrong member address")
}

func TestAddressesWhenCloudConfigEnabled(t *testing.T) {
	shutdownFunc := testutil.CreateCluster(remoteController)
	defer shutdownFunc()

	cfg := hazelcast.NewConfig()
	cloudConfig := config.NewCloudConfig()
	cloudConfig.SetEnabled(true)
	cloudConfig.SetDiscoveryToken("test")
	cfg.NetworkConfig().SetCloudConfig(cloudConfig)
	_, err := hazelcast.NewClientWithConfig(cfg)
	// Since cloudConfig is enabled and it does not have a valid url returning an error means
	// default address is not used.
	assert.Error(t, err)
}

type mapListener struct {
	wg *sync.WaitGroup
}

func (l *mapListener) EntryAdded(event core.EntryEvent) {
	l.wg.Done()
}
