package internal

import (
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization"
)

//type ClientContext interface {
//	ClientConfig() *ClientConfig
//	InvocationService() *InvocationService
//	PartitionService() *PartitionService
//	SerializationService() *SerializationService
//	LifecycleService() *LifecycleService
//	ConnectionManager() *ConnectionManager
//	ListenerService() *ListenerService
//	ClusterService() *ClusterService
//	ProxyManager() *ProxyManager
//	LoadBalancer() *RandomLoadBalancer
//}

type HazelcastClient struct {
	ClientConfig         ClientConfig
	InvocationService    *InvocationService
	PartitionService     *PartitionService
	SerializationService *SerializationService
	LifecycleService     *LifecycleService
	ConnectionManager    *ConnectionManager
	ListenerService      *ListenerService
	ClusterService       *ClusterService
	ProxyManager         *ProxyManager
	LoadBalancer         *RandomLoadBalancer
}

func NewHazelcastClient(config ClientConfig) *HazelcastClient {
	client := HazelcastClient{ClientConfig: config}
	go client.init()
	return &client
}

func (client *HazelcastClient) GetMap(name string) *MapProxy {
	return &MapProxy{}
}

func (client *HazelcastClient) init() {
	client.InvocationService = NewInvocationService(client)
	client.PartitionService = NewPartitionService(client)
}
