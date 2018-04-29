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

// Package hazelcast provides methods for creating Hazelcast clients and client configurations.
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

// NewHazelcastClientWithConfig creates and returns a new IHazelcastInstance with the given config.
// IHazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewHazelcastClientWithConfig(config *config.ClientConfig) (IHazelcastInstance, error) {
	return internal.NewHazelcastClient(config)
}

// NewHazelcastConfig creates and returns a new ClientConfig.
func NewHazelcastConfig() *config.ClientConfig {
	return config.NewClientConfig()
}

// IHazelcastInstance is a Hazelcast instance. Each Hazelcast instance is a member (node) in a cluster.
// Multiple Hazelcast instances can be created.
// Each Hazelcast instance has its own socket, goroutines.
type IHazelcastInstance interface {

	// Map returns the distributed map instance with the specified name.
	Map(name string) (core.IMap, error)

	// List returns the distributed list instance with the specified name.
	List(name string) (core.IList, error)

	// Set returns the distributed set instance with the specified name.
	Set(name string) (core.ISet, error)

	// Topic returns the distributed topic instance with the specified name.
	Topic(name string) (core.ITopic, error)

	// MultiMap returns the distributed multi-map instance with the specified name.
	MultiMap(name string) (core.MultiMap, error)

	// ReplicatedMap returns the replicated map instance with the specified name.
	ReplicatedMap(name string) (core.ReplicatedMap, error)

	// Queue returns the distributed queue instance with the specified name.
	Queue(name string) (core.IQueue, error)

	// Ringbuffer returns the distributed ringbuffer instance with the specified name.
	Ringbuffer(name string) (core.Ringbuffer, error)

	// PNCounter returns the distributed PN (Positive-Negative) CRDT counter instance with the specified name.
	PNCounter(name string) (core.PNCounter, error)

	// FlakeIDGenerator returns the distributed flakeIDGenerator instance with the specified name.
	FlakeIDGenerator(name string) (core.FlakeIDGenerator, error)

	// DistributedObject returns IDistributedObject created by the service with the specified name.
	DistributedObject(serviceName string, name string) (core.IDistributedObject, error)

	// Shutdown shuts down this IHazelcastInstance.
	Shutdown()

	// Cluster returns the ICluster this instance is part of.
	// ICluster interface allows you to add listener for membership
	// events and learn more about the cluster this Hazelcast
	// instance is part of.
	Cluster() core.ICluster

	// Lifecycle returns the lifecycle service for this instance. ILifecycleService allows you
	// to listen for the lifecycle events.
	Lifecycle() core.ILifecycle
}
