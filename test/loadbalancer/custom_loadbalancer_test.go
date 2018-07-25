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

package loadbalancer

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/test"
)

type customLoadBalancer struct {
	clusterService core.Cluster
}

func (clb *customLoadBalancer) Init(cluster core.Cluster) {
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

func TestCustomLoadBalancer(t *testing.T) {

	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)

	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	cfg := hazelcast.NewConfig()
	lb := &customLoadBalancer{}
	cfg.SetLoadBalancer(lb)

	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	addressMp := make(map[core.Member]struct{})
	for i := 0; i < 100; i++ {
		addressMp[lb.Next()] = struct{}{}
	}

	if len(addressMp) != 1 {
		t.Error("Custom loadbalancer should be using only one member.")
	}

}
