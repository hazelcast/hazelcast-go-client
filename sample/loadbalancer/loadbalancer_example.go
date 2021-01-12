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

package main

import (
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
)

type customLoadBalancer struct {
	clusterService core.Cluster
}

func (clb *customLoadBalancer) InitLoadBalancer(cluster core.Cluster) {
	clb.clusterService = cluster
}

func (clb *customLoadBalancer) Next() core.Member {
	members := clb.clusterService.GetMembers()
	size := len(members)
	if size > 0 {
		return members[0]
	}
	return nil
}

func main() {

	cfg := hazelcast.NewConfig()
	lb := &customLoadBalancer{}
	cfg.SetLoadBalancer(lb)

	client, _ := hazelcast.NewClientWithConfig(cfg)

	addressMp := make(map[core.Member]struct{})
	for i := 0; i < 100; i++ {
		addressMp[lb.Next()] = struct{}{}
	}

	// should print 1
	log.Println(len(addressMp))

	client.Shutdown()
}
