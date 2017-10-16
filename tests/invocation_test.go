package tests

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/internal"
	"strconv"
	"testing"
)

func TestNonSmartInvoke(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.SmartRouting = false
	client := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestSingleConnectionWithManyMembers(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.SmartRouting = false
	client := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "testMap"
	mp := client.GetMap(&mapName)
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
}
