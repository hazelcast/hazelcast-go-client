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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/cluster/discovery"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestDiscoveryStrategy(t *testing.T) {
	st := &staticDiscoveryStrategy{}
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			st.addrs = tcx.Cluster.DefaultConfig().Cluster.Network.Addresses
			tcx.Config.Cluster.Discovery.Strategy = st
			tcx.Config.Cluster.Network.Addresses = nil
		},
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		require.True(tcx.T, st.started)
		require.True(tcx.T, tcx.Client.Running())
	})
	require.True(t, st.destroyed)
}

type staticDiscoveryStrategy struct {
	addrs     []string
	attempts  int32
	started   bool
	destroyed bool
}

func (s *staticDiscoveryStrategy) Start(ctx context.Context, opts discovery.StrategyOptions) error {
	if atomic.AddInt32(&s.attempts, 1) < 3 {
		return fmt.Errorf("some error")
	}
	s.started = true
	return nil
}

func (s *staticDiscoveryStrategy) Destroy(ctx context.Context) error {
	s.destroyed = true
	return nil
}

func (s *staticDiscoveryStrategy) DiscoverNodes(ctx context.Context) ([]discovery.Node, error) {
	nodes := make([]discovery.Node, len(s.addrs))
	for i, addr := range s.addrs {
		nodes[i] = discovery.Node{
			PrivateAddr: addr,
		}
	}
	return nodes, nil
}
