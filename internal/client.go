package internal

import (
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/internal/proxy"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type HazelcastClient struct {
	config               hazelcast.ClientConfig
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

func NewHazelcastClient(config hazelcast.ClientConfig) *HazelcastClient {
	client := HazelcastClient{config: config}
	go client.init()
	return &client
}

func (client *HazelcastClient) GetMap(name string) hazelcast.IMap {
	return nil
}

func (client *HazelcastClient) init() {
	client.invocationService = NewInvocationService(client)
	client.partitionService = NewPartitionService(client)

}
