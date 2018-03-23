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
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"math/rand"
	"time"
)

type randomLoadBalancer struct {
	clusterService *clusterService
}

func newRandomLoadBalancer(clusterService *clusterService) *randomLoadBalancer {
	rand.Seed(time.Now().Unix())
	return &randomLoadBalancer{clusterService: clusterService}
}

func (randomLoadBalancer *randomLoadBalancer) nextAddress() *Address {
	membersList := randomLoadBalancer.clusterService.GetMemberList()
	size := len(membersList)
	if size > 0 {
		randomIndex := rand.Intn(size)
		member := membersList[randomIndex]
		return member.Address().(*Address)
	}
	return nil
}
