// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type HazelcastClient struct {
	ClientConfig         *ClientConfig
	InvocationService    *invocationService
	PartitionService     *partitionService
	SerializationService *SerializationService
	LifecycleService     *lifecycleService
	ConnectionManager    *connectionManager
	ListenerService      *listenerService
	ClusterService       *clusterService
	ProxyManager         *proxyManager
	LoadBalancer         *randomLoadBalancer
	HeartBeatService     *heartBeatService
}

func NewHazelcastClient(config *ClientConfig) (*HazelcastClient, error) {
	client := HazelcastClient{ClientConfig: config}
	err := client.init()
	return &client, err
}

func (client *HazelcastClient) GetMap(name string) (core.IMap, error) {
	mp, err := client.GetDistributedObject(SERVICE_NAME_MAP, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.IMap), nil
}

func (client *HazelcastClient) GetList(name string) (core.IList, error) {
	list, err := client.GetDistributedObject(SERVICE_NAME_LIST, name)
	if err != nil {
		return nil, err
	}
	return list.(core.IList), nil
}

func (client *HazelcastClient) GetSet(name string) (core.ISet, error) {
	set, err := client.GetDistributedObject(SERVICE_NAME_SET, name)
	if err != nil {
		return nil, err
	}
	return set.(core.ISet), nil
}
func (client *HazelcastClient) GetReplicatedMap(name string) (core.ReplicatedMap, error) {
	mp, err := client.GetDistributedObject(SERVICE_NAME_REPLICATED_MAP, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.ReplicatedMap), err
}

func (client *HazelcastClient) GetMultiMap(name string) (core.MultiMap, error) {
	mmp, err := client.GetDistributedObject(SERVICE_NAME_MULTI_MAP, name)
	if err != nil {
		return nil, err
	}
	return mmp.(core.MultiMap), err
}

func (client *HazelcastClient) GetFlakeIdGenerator(name string) (core.FlakeIdGenerator, error) {
	flakeIdGenerator, err := client.GetDistributedObject(SERVICE_NAME_ID_GENERATOR, name)
	if err != nil {
		return nil, err
	}
	return flakeIdGenerator.(core.FlakeIdGenerator), err
}

func (client *HazelcastClient) GetTopic(name string) (core.ITopic, error) {
	topic, err := client.GetDistributedObject(SERVICE_NAME_TOPIC, name)
	if err != nil {
		return nil, err
	}
	return topic.(core.ITopic), nil
}

func (client *HazelcastClient) GetQueue(name string) (core.IQueue, error) {
	queue, err := client.GetDistributedObject(SERVICE_NAME_QUEUE, name)
	if err != nil {
		return nil, err
	}
	return queue.(core.IQueue), nil
}

func (client *HazelcastClient) GetRingbuffer(name string) (core.Ringbuffer, error) {
	rb, err := client.GetDistributedObject(SERVICE_NAME_RINGBUFFER_SERVICE, name)
	if err != nil {
		return nil, err
	}
	return rb.(core.Ringbuffer), nil
}

func (client *HazelcastClient) GetLock(name string) (core.ILock, error) {
	lock, err := client.GetDistributedObject(SERVICE_NAME_LOCK, name)
	if err != nil {
		return nil, err
	}
	return lock.(core.ILock), err
}

func (client *HazelcastClient) GetDistributedObject(serviceName string, name string) (core.IDistributedObject, error) {
	var clientProxy, err = client.ProxyManager.getOrCreateProxy(serviceName, name)
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
	client.ConnectionManager = newConnectionManager(client)
	client.HeartBeatService = newHeartBeatService(client)
	client.InvocationService = newInvocationService(client)
	client.ClusterService = newClusterService(client, client.ClientConfig)
	client.ListenerService = newListenerService(client)
	client.PartitionService = newPartitionService(client)
	client.ProxyManager = newProxyManager(client)
	client.LoadBalancer = newRandomLoadBalancer(client.ClusterService)
	client.SerializationService = NewSerializationService(client.ClientConfig.SerializationConfig())
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
