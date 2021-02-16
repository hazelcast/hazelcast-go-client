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

package loadbalancer

import (
	"log"
	"testing"

	hazelcast "github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/core"
	"github.com/hazelcast/hazelcast-go-client/v3/rc"
	"github.com/hazelcast/hazelcast-go-client/v3/test/testutil"
	"github.com/stretchr/testify/assert"
)

var remoteController rc.RemoteController

func TestMain(m *testing.M) {
	var err error
	remoteController, err = rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func TestRandomLoadBalancer(t *testing.T) {

	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)

	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	cfg := hazelcast.NewConfig()
	lb := core.NewRandomLoadBalancer()
	cfg.SetLoadBalancer(lb)

	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	addressMp := make(map[core.Member]struct{})
	// Try 100 times with 2 members, the random loadbalancer should return either of the addresses at some point
	// The chance of only returning of them with a functioning loadbalancer is approximately (1/2)^100,
	// which is negligible.
	for i := 0; i < 100; i++ {
		addressMp[lb.Next()] = struct{}{}
	}
	assert.Equalf(t, 2, len(addressMp), "Random loadbalancer is not using all the members.")

}
