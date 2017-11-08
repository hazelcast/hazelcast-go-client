package internal

import (
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
	. "github.com/hazelcast/go-client/internal/serialization/api"
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
	client.SerializationService = NewSerializationService(NewSerializationConfig(), createBuiltinDataSerializableFactories())
	err := client.ClusterService.start()
	if err != nil {
		return err
	}
	client.HeartBeatService.start()
	client.PartitionService.start()
	client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_STARTED)
	return nil
}

func createBuiltinDataSerializableFactories() map[int32]IdentifiedDataSerializableFactory {
	builtInDataSerializableFactories := make(map[int32]IdentifiedDataSerializableFactory)
	builtInDataSerializableFactories[core.PREDICATE_FACTORY_ID] = createPredicateDataSerializableFactory()
	//builtInDataSerializableFactories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//builtInDataSerializableFactories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	return builtInDataSerializableFactories
}

func createPredicateDataSerializableFactory() IdentifiedDataSerializableFactory {
	idToPredicate := make(map[int32]IdentifiedDataSerializable)
	idToPredicate[0] = &core.SqlPredicate{}
	factory := PredicateFactory{IdToDataSerializable: idToPredicate}
	return &factory

}

func (client *HazelcastClient) Shutdown() {
	if client.LifecycleService.isLive {
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTTING_DOWN)
		client.PartitionService.shutdown()
		client.InvocationService.shutdown()
		client.HeartBeatService.shutdown()
		client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_SHUTDOWN)
	}
}
