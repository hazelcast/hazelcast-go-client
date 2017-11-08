package tests

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/internal"
	"strconv"
	"testing"
	"time"
)

func TestNonSmartInvoke(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.SmartRouting = false
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "myMap"
	mp, _ := client.GetMap(&mapName)
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
	remoteController.ShutdownCluster(cluster.ID)
	client.Shutdown()
}
func TestSingleConnectionWithManyMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.SmartRouting = false
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "testMap"
	mp, _ := client.GetMap(&mapName)
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
		res, err := mp.Get(testKey)
		AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	}
	mp.Clear()
	connectionCount := client.(*internal.HazelcastClient).ConnectionManager.ConnectionCount()
	AssertEqualf(t, nil, int32(1), connectionCount, "Client should open only one connection")
	remoteController.ShutdownCluster(cluster.ID)
	client.Shutdown()
}
func TestInvocationRetry(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	member1, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.SetRedoOperation(true).ConnectionAttemptLimit = 10
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "testMap"
	mp, _ := client.GetMap(&mapName)
	remoteController.ShutdownMember(cluster.ID, member1.UUID)
	//Open the new member in a new subroutine after 5 seconds to ensure that Put will be forced to retry.
	go func() {
		time.Sleep(5 * time.Second)
		remoteController.StartMember(cluster.ID)
	}()
	_, err := mp.Put("testKey", "testValue")
	AssertNilf(t, err, nil, "InvocationRetry failed")
	result, err := mp.Get("testKey")
	AssertEqualf(t, err, result, "testValue", "invocation retry failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
