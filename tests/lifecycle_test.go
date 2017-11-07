package tests

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/internal"
	"sync"
	"testing"
)

type lifecycyleListener struct {
	wg        *sync.WaitGroup
	collector []string
}

func (lifecycyleListener *lifecycyleListener) LifecycleStateChanged(newState string) {
	lifecycyleListener.collector = append(lifecycyleListener.collector, newState)
	lifecycyleListener.wg.Done()
}
func TestLifecycleListener(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	config := hazelcast.NewHazelcastConfig()
	lifecycleListener := lifecycyleListener{wg: wg, collector: make([]string, 0)}
	config.AddLifecycleListener(&lifecycleListener)
	remoteController.StartMember(cluster.ID)
	wg.Add(5)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	client.Shutdown()
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[0], internal.LIFECYCLE_STATE_STARTING, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[1], internal.LIFECYCLE_STATE_CONNECTED, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[2], internal.LIFECYCLE_STATE_STARTED, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[3], internal.LIFECYCLE_STATE_SHUTTING_DOWN, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[4], internal.LIFECYCLE_STATE_SHUTDOWN, "Lifecycle listener failed")
	remoteController.ShutdownCluster(cluster.ID)
}
func TestLifecycleListenerForDisconnected(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	lifecycleListener := lifecycyleListener{wg: wg, collector: make([]string, 0)}
	remoteController.StartMember(cluster.ID)
	wg.Add(1)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig.ConnectionAttemptLimit = 10
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	registrationId := client.(*internal.HazelcastClient).LifecycleService.AddListener(&lifecycleListener)
	remoteController.ShutdownCluster(cluster.ID)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "Lifecycle listener failed")
	AssertEqualf(t, nil, lifecycleListener.collector[0], internal.LIFECYCLE_STATE_DISCONNECTED, "Lifecycle listener failed")
	client.GetLifecycle().RemoveListener(&registrationId)
	client.Shutdown()
}

func TestRemoveListener(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	lifecycleListener := lifecycyleListener{wg: wg, collector: make([]string, 0)}
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	registrationId := client.GetLifecycle().AddListener(&lifecycleListener)
	wg.Add(2)
	client.GetLifecycle().RemoveListener(&registrationId)
	client.Shutdown()
	timeout := WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "Lifecycle listener failed")
	AssertEqualf(t, nil, len(lifecycleListener.collector), 0, "Lifecycle addListener or removeListener failed")
	remoteController.ShutdownCluster(cluster.ID)
}
