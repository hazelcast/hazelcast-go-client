package internal

import (
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type HazelcastClient struct {
	ClientConfig         *ClientConfig
	InvocationService    *InvocationService
	PartitionService     *PartitionService
	SerializationService *SerializationService
	LifecycleService     *LifecycleService
	ConnectionManager    *ConnectionManager
	ListenerService      *ListenerService
	ClusterService       *ClusterService
	ProxyManager         *ProxyManager
	LoadBalancer         *RandomLoadBalancer
	HeartBeatService     *HeartBeatService
}

func NewHazelcastClient(config *ClientConfig) (*HazelcastClient, error) {
	client := HazelcastClient{ClientConfig: config}
	err := client.init()
	return &client, err
}

func (client *HazelcastClient) GetMap(name *string) (core.IMap, error) {
	mapService := SERVICE_NAME_MAP
	mp, err := client.GetDistributedObject(&mapService, name)
	return mp.(core.IMap), err
}

func (client *HazelcastClient) GetDistributedObject(serviceName *string, name *string) (core.IDistributedObject, error) {
	var clientProxy, err = client.ProxyManager.GetOrCreateProxy(serviceName, name)
	if err != nil {
		return nil, err
	}
	return clientProxy, nil
}

func (client *HazelcastClient) GetCluster() core.ICluster {
	return client.ClusterService
}

func (client *HazelcastClient) GetLifecycle() core.ILifecycle {
	return client.LifecycleService
}

func (client *HazelcastClient) init() error {
	client.LifecycleService = newLifecycleService(client.ClientConfig)
	client.ConnectionManager = NewConnectionManager(client)
	client.HeartBeatService = newHeartBeatService(client)
	client.InvocationService = NewInvocationService(client)
	client.ListenerService = newListenerService(client)
	client.ClusterService = NewClusterService(client, client.ClientConfig)
	client.PartitionService = NewPartitionService(client)
	client.ProxyManager = newProxyManager(client)
	client.LoadBalancer = NewRandomLoadBalancer(client.ClusterService)
	client.SerializationService = NewSerializationService(client.ClientConfig.SerializationConfig)
	err := client.ClusterService.start()
	if err != nil {
		return err
	}
	client.HeartBeatService.start()
	client.PartitionService.start()
	client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_STARTED)
	return nil
}

func (client *HazelcastClient) Shutdown() {
	if client.LifecycleService.isLive.Load().(bool) {
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTTING_DOWN)
		client.PartitionService.shutdown()
		client.InvocationService.shutdown()
		client.HeartBeatService.shutdown()
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTDOWN)
	}
}
