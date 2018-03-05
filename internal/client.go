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

func (client *HazelcastClient) GetMap(name string) (core.IMap, error) {
	mapService := SERVICE_NAME_MAP
	mp, err := client.GetDistributedObject(mapService, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.IMap), err
}

func (client *HazelcastClient) GetList(name string) (core.IList, error) {
	listService := SERVICE_NAME_LIST
	list, err := client.GetDistributedObject(listService, name)
	if err != nil {
		return nil, err
	}
	return list.(core.IList), err
}

func (client *HazelcastClient) GetSet(name string) (core.ISet, error) {
	setService := SERVICE_NAME_SET
	set, err := client.GetDistributedObject(setService, name)
	if err != nil {
		return nil, err
	}
	return set.(core.ISet), err
}

func (client *HazelcastClient) GetTopic(name string) (core.ITopic, error) {
	topicService := SERVICE_NAME_TOPIC
	topic, err := client.GetDistributedObject(topicService, name)
	if err != nil {
		return nil, err
	}
	return topic.(core.ITopic), err
}

func (client *HazelcastClient) GetDistributedObject(serviceName string, name string) (core.IDistributedObject, error) {
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
	client.ClusterService = NewClusterService(client, client.ClientConfig)
	client.ListenerService = newListenerService(client)
	client.PartitionService = NewPartitionService(client)
	client.ProxyManager = newProxyManager(client)
	client.LoadBalancer = NewRandomLoadBalancer(client.ClusterService)
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
