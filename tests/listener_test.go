package tests

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

func TestListenerWhenNodeLeftAndReconnected(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DefaultServerConfig)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	assert.Equal(t, err, nil, nil)
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
	assert.Equalf(t, nil, false, timeout, "listener reregister failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListenerWithMultipleMembers(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	assert.Equal(t, err, nil, nil)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, nil, false, timeout, "smartListener with multiple members failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestListenerWithMemberConnectedAfterAWhile(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	entryAdded := &mapListener{wg: wg}
	mp, _ := client.GetMap("testMap")
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	assert.Equal(t, err, nil, nil)
	remoteController.StartMember(cluster.ID)
	time.Sleep(15 * time.Second) // Wait for partitionTable update
	wg.Add(100)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
	}
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, nil, false, timeout, "smartListener adding a member after a while failed to listen.")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
