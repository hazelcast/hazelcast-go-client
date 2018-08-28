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

// NewClient creates and returns a new Instance.
// Hazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClient() (Instance, error) {
	return NewClientWithConfig(config.New())
}

// NewClientWithConfig creates and returns a new Instance with the given config.
// Hazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClientWithConfig(config *config.Config) (Instance, error) {
	return internal.NewHazelcastClient(config)
}

// NewConfig creates and returns a new config.
func NewConfig() *config.Config {
	return config.New()
}

// Instance is a Hazelcast instance. Each Hazelcast instance is a member (node) in a cluster.
// Multiple Hazelcast instances can be created.
// Each Hazelcast instance has its own socket, goroutines.
type Instance interface {

	// GetMap returns the distributed map instance with the specified name.
	GetMap(name string) (core.Map, error)

	// GetList returns the distributed list instance with the specified name.
	GetList(name string) (core.List, error)

	// GetSet returns the distributed set instance with the specified name.
	GetSet(name string) (core.Set, error)

	// GetTopic returns the distributed topic instance with the specified name.
	GetTopic(name string) (core.Topic, error)

	// GetMultiMap returns the distributed multi-map instance with the specified name.
	GetMultiMap(name string) (core.MultiMap, error)

	// GetReplicatedMap returns the replicated map instance with the specified name.
	GetReplicatedMap(name string) (core.ReplicatedMap, error)

	// GetQueue returns the distributed queue instance with the specified name.
	GetQueue(name string) (core.Queue, error)

	// GetRingbuffer returns the distributed ringbuffer instance with the specified name.
	GetRingbuffer(name string) (core.Ringbuffer, error)

	// GetPNCounter returns the distributed PN (Positive-Negative) CRDT counter instance with the specified name.
	GetPNCounter(name string) (core.PNCounter, error)

	// GetFlakeIDGenerator returns the distributed flakeIDGenerator instance with the specified name.
	GetFlakeIDGenerator(name string) (core.FlakeIDGenerator, error)

	// GetDistributedObject returns DistributedObject created by the service with the specified name.
	GetDistributedObject(serviceName string, name string) (core.DistributedObject, error)

	// Shutdown shuts down this Instance.
	Shutdown()

	// ClusterService returns the Cluster service  this instance is part of.
	// Cluster interface allows you to add listener for membership
	// events and learn more about the cluster this Hazelcast
	// instance is part of.
	ClusterService() core.Cluster

	// LifecycleService returns the lifecycle service for this instance. Lifecycle service allows you
	// to listen for the lifecycle events.
	LifecycleService() core.Lifecycle

	// Config returns the configuration of this client
	Config() *config.Config
}
