/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestRoundRobinLoadBalancer_OneOf(t *testing.T) {
	var lb cluster.LoadBalancer = cluster.NewRoundRobinLoadBalancer()
	targetAddrs := []cluster.Address{"a:5071", "b:5701", "c:5701", "a:5071", "b:5701"}
	var addrs []cluster.Address
	for i := 0; i < 5; i++ {
		addrs = append(addrs, lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"}))
	}
	assert.Equal(t, targetAddrs, addrs)
}

func TestRoundRobinLoadBalancer_OneOf_ConnectionLost(t *testing.T) {
	lb := cluster.NewRoundRobinLoadBalancer()
	// directly set the index
	(*lb) = 5
	addr := lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"})
	// since the index is greater than available addresses, LB should return the last address
	assert.Equal(t, cluster.Address("c:5701"), addr)
}

func TestNewRandomLoadBalancer(t *testing.T) {
	var lb cluster.LoadBalancer = cluster.NewRandomLoadBalancer()
	addr := lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"})
	if addr.Equal("") {
		t.Fatalf("one of the addresses must be returned")
	}
}

func TestRandomLoadBalancer_OneOf(t *testing.T) {
	var lb cluster.LoadBalancer
	// seed the random load balancer with a fixed int
	r := rand.New(rand.NewSource(5))
	rlb := cluster.RandomLoadBalancer(*r)
	lb = &rlb
	targetAddrs := []cluster.Address{"a:5071", "b:5701", "b:5701", "b:5701", "a:5071"}
	var addrs []cluster.Address
	for i := 0; i < 5; i++ {
		addrs = append(addrs, lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"}))
	}
	assert.Equal(t, targetAddrs, addrs)
}
