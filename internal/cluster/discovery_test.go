/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/stretchr/testify/require"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/cluster/discovery"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type dummyDiscoverStrategy struct {
	nodes []discovery.Node
}

func (d dummyDiscoverStrategy) DiscoverNodes(ctx context.Context) ([]discovery.Node, error) {
	return d.nodes, nil
}

func TestDiscoveryStrategyAdapter_Addresses(t *testing.T) {
	testCases := []struct {
		name     string
		nodes    []discovery.Node
		target   []pubcluster.Address
		publicIP bool
	}{
		{
			name: "no public or private addreses",
			nodes: []discovery.Node{
				{},
			},
			target: []pubcluster.Address{},
		},
		{
			name: "no public ip with public and private adresses",
			nodes: []discovery.Node{
				{PrivateAddr: "10.10.10.10:5000", PublicAddr: "20.10.10.10:5000"},
				{PrivateAddr: "10.11.11.11:5000", PublicAddr: "20.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"10.10.10.10:5000",
				"10.11.11.11:5000",
			},
		},
		{
			name: "no public ip with only private adresses",
			nodes: []discovery.Node{
				{PrivateAddr: "10.10.10.10:5000"},
				{PrivateAddr: "10.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"10.10.10.10:5000",
				"10.11.11.11:5000",
			},
		},
		{
			name: "no public ip with only public adresses",
			nodes: []discovery.Node{
				{PublicAddr: "20.10.10.10:5000"},
				{PublicAddr: "20.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"20.10.10.10:5000",
				"20.11.11.11:5000",
			},
		},
		{
			name:     "public ip with public and private adresses",
			publicIP: true,
			nodes: []discovery.Node{
				{PrivateAddr: "10.10.10.10:5000", PublicAddr: "20.10.10.10:5000"},
				{PrivateAddr: "10.11.11.11:5000", PublicAddr: "20.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"20.10.10.10:5000",
				"20.11.11.11:5000",
			},
		},
		{
			name:     "public ip with only private adresses",
			publicIP: true,
			nodes: []discovery.Node{
				{PrivateAddr: "10.10.10.10:5000"},
				{PrivateAddr: "10.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"10.10.10.10:5000",
				"10.11.11.11:5000",
			},
		},
		{
			name:     "public ip with only public adresses",
			publicIP: true,
			nodes: []discovery.Node{
				{PublicAddr: "20.10.10.10:5000"},
				{PublicAddr: "20.11.11.11:5000"},
			},
			target: []pubcluster.Address{
				"20.10.10.10:5000",
				"20.11.11.11:5000",
			},
		},
	}
	lg := logger.LogAdaptor{Logger: logger.New()}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			st := dummyDiscoverStrategy{nodes: tc.nodes}
			cfg := pubcluster.DiscoveryConfig{
				UsePublicIP: tc.publicIP,
				Strategy:    st,
			}
			ad := cluster.NewDiscoveryStrategyAdapter(cfg, lg)
			addrs, err := ad.Addresses(context.TODO())
			require.NoError(t, err)
			require.Equal(t, tc.target, addrs)
		})
	}

}
