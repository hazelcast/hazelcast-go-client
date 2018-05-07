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
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type HazelcastClient struct {
	ClientConfig         *config.ClientConfig
	InvocationService    invocationService
	PartitionService     *partitionService
	SerializationService *serialization.Service
	LifecycleService     *lifecycleService
	ConnectionManager    connectionManager
	ListenerService      *listenerService
	ClusterService       *clusterService
	ProxyManager         *proxyManager
	LoadBalancer         *randomLoadBalancer
	HeartBeatService     *heartBeatService
}

func NewHazelcastClient(config *config.ClientConfig) (*HazelcastClient, error) {
	client := HazelcastClient{ClientConfig: config}
	err := client.init()
	return &client, err
}

func (c *HazelcastClient) GetMap(name string) (core.Map, error) {
	mp, err := c.GetDistributedObject(bufutil.ServiceNameMap, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.Map), nil
}

func (c *HazelcastClient) GetList(name string) (core.List, error) {
	list, err := c.GetDistributedObject(bufutil.ServiceNameList, name)
	if err != nil {
		return nil, err
	}
	return list.(core.List), nil
}

func (c *HazelcastClient) GetSet(name string) (core.Set, error) {
	set, err := c.GetDistributedObject(bufutil.ServiceNameSet, name)
	if err != nil {
		return nil, err
	}
	return set.(core.Set), nil
}

func (c *HazelcastClient) GetReplicatedMap(name string) (core.ReplicatedMap, error) {
	mp, err := c.GetDistributedObject(bufutil.ServiceNameReplicatedMap, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.ReplicatedMap), err
}

func (c *HazelcastClient) GetMultiMap(name string) (core.MultiMap, error) {
	mmp, err := c.GetDistributedObject(bufutil.ServiceNameMultiMap, name)
	if err != nil {
		return nil, err
	}
	return mmp.(core.MultiMap), err
}

func (c *HazelcastClient) GetFlakeIDGenerator(name string) (core.FlakeIDGenerator, error) {
	flakeIDGenerator, err := c.GetDistributedObject(bufutil.ServiceNameIDGenerator, name)
	if err != nil {
		return nil, err
	}
	return flakeIDGenerator.(core.FlakeIDGenerator), err
}

func (c *HazelcastClient) GetTopic(name string) (core.Topic, error) {
	topic, err := c.GetDistributedObject(bufutil.ServiceNameTopic, name)
	if err != nil {
		return nil, err
	}
	return topic.(core.Topic), nil
}

func (c *HazelcastClient) GetQueue(name string) (core.Queue, error) {
	queue, err := c.GetDistributedObject(bufutil.ServiceNameQueue, name)
	if err != nil {
		return nil, err
	}
	return queue.(core.Queue), nil
}

func (c *HazelcastClient) GetRingbuffer(name string) (core.Ringbuffer, error) {
	rb, err := c.GetDistributedObject(bufutil.ServiceNameRingbufferService, name)
	if err != nil {
		return nil, err
	}
	return rb.(core.Ringbuffer), nil
}

func (c *HazelcastClient) GetPNCounter(name string) (core.PNCounter, error) {
	counter, err := c.GetDistributedObject(bufutil.ServiceNamePNCounter, name)
	if err != nil {
		return nil, err
	}
	return counter.(core.PNCounter), nil
}

func (c *HazelcastClient) GetDistributedObject(serviceName string, name string) (core.DistributedObject, error) {
	var clientProxy, err = c.ProxyManager.getOrCreateProxy(serviceName, name)
	if err != nil {
		return nil, err
	}
	return clientProxy, nil
}

func (c *HazelcastClient) GetCluster() core.Cluster {
	return c.ClusterService
}

func (c *HazelcastClient) GetLifecycle() core.Lifecycle {
	return c.LifecycleService
}

func (c *HazelcastClient) init() error {
	c.LifecycleService = newLifecycleService(c.ClientConfig)
	c.ConnectionManager = newConnectionManager(c)
	c.HeartBeatService = newHeartBeatService(c)
	c.InvocationService = newInvocationService(c)
	c.ClusterService = newClusterService(c, c.ClientConfig)
	c.ListenerService = newListenerService(c)
	c.PartitionService = newPartitionService(c)
	c.ProxyManager = newProxyManager(c)
	c.LoadBalancer = newRandomLoadBalancer(c.ClusterService)
	var err error
	c.SerializationService, err = serialization.NewSerializationService(c.ClientConfig.SerializationConfig())
	if err != nil {
		return err
	}
	err = c.ClusterService.start()
	if err != nil {
		return err
	}
	c.HeartBeatService.start()
	c.PartitionService.start()
	c.LifecycleService.fireLifecycleEvent(LifecycleStateStarted)
	return nil
}

func (c *HazelcastClient) Shutdown() {
	if c.LifecycleService.isLive.Load().(bool) {
		c.LifecycleService.fireLifecycleEvent(LifecycleStateShuttingDown)
		c.ConnectionManager.shutdown()
		c.PartitionService.shutdown()
		c.ClusterService.shutdown()
		c.InvocationService.shutdown()
		c.HeartBeatService.shutdown()
		c.ListenerService.shutdown()
		c.LifecycleService.fireLifecycleEvent(LifecycleStateShutdown)
	}
}
