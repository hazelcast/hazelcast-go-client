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

// Package Hazelcast provides methods for creating Hazelcast clients and client configurations.
package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
)

// NewHazelcastClient creates and returns a new IHazelcastInstance.
// IHazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewHazelcastClient() (IHazelcastInstance, error) {
	return NewHazelcastClientWithConfig(config.NewClientConfig())
}

// NewHazelcastClient creates and returns a new IHazelcastInstance with the given config.
// IHazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewHazelcastClientWithConfig(config *config.ClientConfig) (IHazelcastInstance, error) {
	return internal.NewHazelcastClient(config)
}

// NewHazelcsatConfig creates and returns a new ClientConfig.
func NewHazelcastConfig() *config.ClientConfig {
	return config.NewClientConfig()
}

// IHazelcastInstance is a Hazelcast instance. Each Hazelcast instance is a member (node) in a cluster.
// Multiple Hazelcast instances can be created.
// Each Hazelcast instance has its own socket, goroutines.
type IHazelcastInstance interface {

	// GetMap returns the distributed map instance with the specified name.
	GetMap(name string) (core.IMap, error)

	// GetList returns the distributed list instance with the specified name.
	GetList(name string) (core.IList, error)

	// GetSet returns the distributed set instance with the specified name.
	GetSet(name string) (core.ISet, error)

	// GetTopic returns the distributed topic istance with the specified name.
	GetTopic(name string) (core.ITopic, error)

	// GetMultiMap returns the distributed multi-map instance with the specified name.
	GetMultiMap(name string) (core.MultiMap, error)

	// GetReplicatedMap returns the replicated map instance with the specified name.
	GetReplicatedMap(name string) (core.ReplicatedMap, error)

	// GetQueue returns the distributed queue instance with the specified name.
	GetQueue(name string) (core.IQueue, error)

	// GetRingbuffer returns the distributed ringbuffer instance with the specified name.
	GetRingbuffer(name string) (core.Ringbuffer, error)

	// GetPNCounter returns the distributed PN (Positive-Negative) CRDT counter instance with the specified name.
	GetPNCounter(name string) (core.PNCounter, error)

	// GetFlakeIdGenerator returns the distributed flakeIdGenerator instance with the specified name.
	GetFlakeIdGenerator(name string) (core.FlakeIdGenerator, error)

	// GetDistributedObject returns IDistributedObject created by the service with the specified name.
	GetDistributedObject(serviceName string, name string) (core.IDistributedObject, error)

	// Shutdown shuts down this IHazelcastInstance.
	Shutdown()

	// GetCluster returns the ICluster this instance is part of.
	// ICluster interface allows you to add listener for membership
	// events and learn more about the cluster this Hazelcast
	// instance is part of.
	GetCluster() core.ICluster

	// GetLifecycle returns the lifecycle service for this instance. ILifecycleService allows you
	// to listen for the lifecycle events.
	GetLifecycle() core.ILifecycle
}
