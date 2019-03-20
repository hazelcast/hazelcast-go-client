// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRoundRobinLoadBalancer(t *testing.T) {

	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)

	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	cfg := hazelcast.NewConfig()
	lb := core.NewRoundRobinLoadBalancer()
	cfg.SetLoadBalancer(lb)

	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	addressMp := make(map[core.Member]struct{})
	expected := len(client.Cluster().GetMembers())
	for i := 0; i < expected; i++ {
		addressMp[lb.Next()] = struct{}{}
	}
	assert.Equalf(t, len(addressMp), expected, "RoundRobin loadbalancer is not using members one by one")

}

func TestRoundRobinLoadBalancerOrder(t *testing.T) {

	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)

	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	cfg := hazelcast.NewConfig()
	lb := core.NewRoundRobinLoadBalancer()
	cfg.SetLoadBalancer(lb)

	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	expected := client.Cluster().GetMembers()
	for j := 0; j < 50; j++ {
		for i := 0; i < len(expected); i++ {
			member := lb.Next()
			assert.Equalf(t, member.UUID(), expected[i].UUID(), "RoundRobin loadbalancer is not going in order.")
		}
	}

}
