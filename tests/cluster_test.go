package tests

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/rc"
	"log"
	"strconv"
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
	client := hazelcast.NewHazelcastClientWithConfig(config)
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
	client := hazelcast.NewHazelcastClientWithConfig(config)
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
	client := hazelcast.NewHazelcastClient()
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
func TestGetMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	member2, _ := remoteController.StartMember(cluster.ID)
	member3, _ := remoteController.StartMember(cluster.ID)
	client := hazelcast.NewHazelcastClient()
	members := client.GetCluster().GetMemberList()
	AssertEqualf(t, nil, len(members), 3, "GetMemberList returned wrong number of members")
	client.Shutdown()
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.ShutdownMember(cluster.ID, member2.UUID)
	remoteController.ShutdownMember(cluster.ID, member3.UUID)
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListenerReregister(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	client := hazelcast.NewHazelcastClient()
	entryAdded := &mapListener{wg: wg}
	mapName := "testMap"
	mp, _ := client.GetMap(&mapName)
	_, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	remoteController.StartMember(cluster.ID)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "listener reregister failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

type mapListener struct {
	wg *sync.WaitGroup
}

func (ml *mapListener) EntryAdded(event core.IEntryEvent) {
	ml.wg.Done()
}
