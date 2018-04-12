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
	"github.com/hazelcast/go-hazelcast/config"
	"github.com/hazelcast/go-hazelcast/core"
	"github.com/hazelcast/go-hazelcast/internal"
)

// NewClient creates and returns a new Client.
// IHazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClient() (Client, error) {
	return NewClientWithConfig(config.NewConfig())
}

// NewClient creates and returns a new Client with the given config.
// IHazelcast instance enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClientWithConfig(config *config.Config) (Client, error) {
	return internal.NewClient(config)
}

// NewConfig creates and returns a new Config.
func NewConfig() *config.Config {
	return config.NewConfig()
}

// Client is a Hazelcast instance. Each Hazelcast instance is a member (node) in a cluster.
// Multiple Hazelcast instances can be created.
// Each Hazelcast instance has its own socket, goroutines.
type Client interface {

	// Map returns the distributed map instance with the specified name.
	Map(name string) (core.Map, error)

	// List returns the distributed list instance with the specified name.
	List(name string) (core.List, error)

	// Set returns the distributed set instance with the specified name.
	Set(name string) (core.Set, error)

	// Topic returns the distributed topic istance with the specified name.
	Topic(name string) (core.Topic, error)

	// MultiMap returns the distributed multi-map instance with the specified name.
	MultiMap(name string) (core.MultiMap, error)

	// ReplicatedMap returns the replicated map instance with the specified name.
	ReplicatedMap(name string) (core.ReplicatedMap, error)

	// Queue returns the distributed queue instance with the specified name.
	Queue(name string) (core.Queue, error)

	// Ringbuffer returns the distributed ringbuffer instance with the specified name.
	Ringbuffer(name string) (core.Ringbuffer, error)

	// PNCounter returns the distributed PN (Positive-Negative) CRDT counter instance with the specified name.
	PNCounter(name string) (core.PNCounter, error)

	// FlakeIdGenerator returns the distributed flakeIdGenerator instance with the specified name.
	FlakeIdGenerator(name string) (core.FlakeIdGenerator, error)

	// DistributedObject returns IDistributedObject created by the service with the specified name.
	DistributedObject(serviceName string, name string) (core.DistributedObject, error)

	// Shutdown shuts down this Client.
	Shutdown()

	// Cluster returns the ICluster this instance is part of.
	// ICluster interface allows you to add listener for membership
	// events and learn more about the cluster this Hazelcast
	// instance is part of.
	Cluster() core.Cluster

	// Lifecycle returns the lifecycle service for this instance. ILifecycleService allows you
	// to listen for the lifecycle events.
	Lifecycle() core.Lifecycle
}
