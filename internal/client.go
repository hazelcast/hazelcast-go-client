package internal

import (
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal/proxy"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type ClientContext interface {
	config() ClientConfig
	invocationService() *InvocationService
	partitionService() *PartitionService
	serializationService() *SerializationService
	lifecycleService() *LifecycleService
	connectionManager() *ConnectionManager
	listenerService() *ListenerService
	clusterService() *ClusterService
	proxyManager() *proxy.Manager
	loadBalancer() *RandomLoadBalancer
}

type HazelcastClient struct {
	config               ClientConfig
	invocationService    *InvocationService
	partitionService     *PartitionService
	serializationService *SerializationService
	lifecycleService     *LifecycleService
	connectionManager    *ConnectionManager
	listenerService      *ListenerService
	clusterService       *ClusterService
	proxyManager         *proxy.Manager
	loadBalancer         *RandomLoadBalancer
}

func NewHazelcastClient(config ClientConfig) *HazelcastClient {
	client := HazelcastClient{config: config}
	go client.init()
	return &client
}

func (client *HazelcastClient) GetMap(name string) proxy.Map {
	return nil
}

func (client *HazelcastClient) init() {
	client.invocationService = NewInvocationService(client)
	client.partitionService = NewPartitionService(client)
}

func (client *HazelcastClient) init()