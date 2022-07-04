/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	"context"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestConfig_SetLoadBalancer(t *testing.T) {
	// test needs information about current member loads.
	// this information is needed to be supplied through rc.
	var (
		lb cluster.LoadBalancer = cluster.NewRoundRobinLoadBalancer()
		//targetAddrs                      = []cluster.Address{"localhost:17001", "localhost:17002", "localhost:17003"}
	)
	//var addrs []cluster.Address
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions("cluster_SetLoadBalancer", 17001, 3)
	cfg := cls.DefaultConfig()
	cfg.Cluster.SetLoadBalancer(lb)
	client, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)

	}
	defer func(ctx context.Context, client *hazelcast.Client) {
		err = client.Shutdown(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}(ctx, client)
}
