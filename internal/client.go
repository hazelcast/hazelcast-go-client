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

func NewHazelcastClient(config *ClientConfig) *HazelcastClient {
	client := HazelcastClient{ClientConfig: config}
	//go client.init()
	client.init()
	return &client
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

func (client *HazelcastClient) init() {
	client.InvocationService = NewInvocationService(client)
	client.PartitionService = NewPartitionService(client)
	client.ClusterService = NewClusterService(client, client.ClientConfig)
	client.LoadBalancer = NewRandomLoadBalancer(client.ClusterService)
	client.SerializationService = NewSerializationService(NewSerializationConfig())
	client.ConnectionManager = NewConnectionManager(client)
	client.LifecycleService = newLifecycleService(client.ClientConfig)
	client.ListenerService = newListenerService(client)
	client.HeartBeatService = newHeartBeatService(client)
	client.ProxyManager = newProxyManager(client)
	client.ClusterService.start()
	client.HeartBeatService.start()
	client.PartitionService.start()
	client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_STARTED)
}

func (client *HazelcastClient) Shutdown() {
	if client.LifecycleService.isLive {
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTTING_DOWN)
		client.PartitionService.shutdown()
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTDOWN)
	}
}
