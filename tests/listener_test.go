package tests

import (
	"github.com/hazelcast/hazelcast-go-client"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestListenerWhenNodeLeftAndReconnected(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
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
	AssertEqualf(t, nil, false, timeout, "listener reregister failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestListenerWithMultipleMembers(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	defer remoteController.ShutdownCluster(cluster.ID)
	defer client.Shutdown()
	defer mp.Clear()
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	defer mp.RemoveEntryListener(registrationId)
	AssertEqual(t, err, nil, nil)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "smartListener with multiple members failed")

}
func TestListenerWithMemberConnectedAfterAWhile(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	defer remoteController.ShutdownCluster(cluster.ID)
	defer client.Shutdown()
	defer mp.Clear()
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	defer mp.RemoveEntryListener(registrationId)
	AssertEqual(t, err, nil, nil)
	remoteController.StartMember(cluster.ID)
	time.Sleep(15 * time.Second) // Wait for partitionTable update
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "smartListener adding a member after a while failed to listen.")

}
