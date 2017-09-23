package tests

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/rc"
	"log"
	"sync"
	"testing"
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
	hazelcast.NewHazelcastClientWithConfig(config)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Cluster initialMembershipListener failed")
	remoteController.ShutdownCluster(cluster.ID)
}
func TestMemberAddedandRemoved(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.AddMembershipListener(&membershipListener{wg: wg})
	wg.Add(1)
	hazelcast.NewHazelcastClientWithConfig(config)
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
	remoteController.ShutdownCluster(cluster.ID)
}
