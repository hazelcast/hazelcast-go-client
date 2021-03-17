// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package core

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// RandomLoadBalancer is a loadbalancer that selects members randomly from the given cluster service.
type RandomLoadBalancer struct {
	LoadBalancer
	InitialMembershipListener
	membersRef     atomic.Value
	clusterService Cluster
}

// NewRandomLoadBalancer creates and returns a RandomLoadBalancer.
func NewRandomLoadBalancer() *RandomLoadBalancer {
	rand.Seed(time.Now().Unix())
	return &RandomLoadBalancer{}
}

func (b *RandomLoadBalancer) InitLoadBalancer(cluster Cluster) {
	b.clusterService = cluster
	b.clusterService.AddMembershipListener(b)
}

func (b *RandomLoadBalancer) Init(event InitialMembershipEvent) {
	b.setMembersRef()
}

func (b *RandomLoadBalancer) Next() Member {
	membersList := b.getMembers()
	if membersList == nil {
		return nil
	}
	size := len(membersList)
	if size > 0 {
		randomIndex := rand.Intn(size)
		return membersList[randomIndex]
	}
	return nil
}

func (b *RandomLoadBalancer) MemberAdded(membershipEvent MembershipEvent) {
	b.setMembersRef()
}

func (b *RandomLoadBalancer) MemberRemoved(membershipEvent MembershipEvent) {
	b.setMembersRef()
}

func (b *RandomLoadBalancer) setMembersRef() {
	b.membersRef.Store(b.clusterService.GetMemberList())
}

func (b *RandomLoadBalancer) getMembers() []Member {
	membersRef := b.membersRef.Load()
	if membersRef == nil {
		return nil
	}
	return membersRef.([]Member)
}

// RoundRobinLoadBalancer is a loadbalancer where members are used with round robin logic.
type RoundRobinLoadBalancer struct {
	LoadBalancer
	InitialMembershipListener
	clusterService Cluster
	membersRef     atomic.Value
	index          int64
}

// NewRoundRobinLoadBalancer creates and returns a RoundLobinLoadBalancer.
func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{}
}

func (rrl *RoundRobinLoadBalancer) InitLoadBalancer(cluster Cluster) {
	rrl.clusterService = cluster
	rrl.clusterService.AddMembershipListener(rrl)
}

func (rrl *RoundRobinLoadBalancer) Init(event InitialMembershipEvent) {
	rrl.setMembersRef()
}

func (rrl *RoundRobinLoadBalancer) MemberAdded(membershipEvent MembershipEvent) {
	rrl.setMembersRef()
}

func (rrl *RoundRobinLoadBalancer) MemberRemoved(membershipEvent MembershipEvent) {
	rrl.setMembersRef()
}

func (rrl *RoundRobinLoadBalancer) setMembersRef() {
	rrl.membersRef.Store(rrl.clusterService.GetMemberList())
}

func (b *RoundRobinLoadBalancer) getMembers() []Member {
	membersRef := b.membersRef.Load()
	if membersRef == nil {
		return nil
	}
	return membersRef.([]Member)
}

func (rrl *RoundRobinLoadBalancer) Next() Member {
	members := rrl.clusterService.GetMemberList()
	size := int64(len(members))
	if size > 0 {
		index := getAndIncrement(&rrl.index) % size
		return members[index]
	}
	return nil
}

func getAndIncrement(val *int64) int64 {
	newVal := atomic.AddInt64(val, 1)
	return newVal - 1
}
