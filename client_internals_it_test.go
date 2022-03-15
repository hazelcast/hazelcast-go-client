//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

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

package hazelcast_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Tests that require the hazelcastinternal tag.

func TestListenersAfterClientDisconnected(t *testing.T) {
	t.Run("MemberHostname_ClientIP", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "127.0.0.1", 46501)
	})
	t.Run("MemberHostname_ClientHostname", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "localhost", 47501)
	})
	t.Run("MemberIP_ClientIP", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "127.0.0.1", 48501)
	})
	t.Run("MemberIP_ClientHostname", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "localhost", 49501)
	})
}

func TestNotReceivedInvocation(t *testing.T) {
	// This test skips sending an invocation to the member in order to simulate lost connection.
	// After 5 seconds, a GroupLost event is published to simulate the disconnection.
	clientTester(t, func(t *testing.T, smart bool) {
		tc := it.StartNewClusterWithOptions("not-received-invocation", 55701, 1)
		defer tc.Shutdown()
		ctx := context.Background()
		config := tc.DefaultConfig()
		config.Cluster.Unisocket = !smart
		client := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
		defer client.Shutdown(ctx)
		ci := hz.NewClientInternal(client)
		var cnt int32
		handler := newRiggedInvocationHandler(ci.InvocationHandler(), func(inv invocation.Invocation) bool {
			if inv.Request().Type() == codec.MapSetCodecRequestMessageType {
				return atomic.AddInt32(&cnt, 1) != 1
			}
			return true
		})
		ci.InvocationService().SetHandler(handler)
		m := it.MustValue(client.GetMap(ctx, "test-map")).(*hz.Map)
		go func() {
			time.Sleep(5 * time.Second)
			conns := ci.ConnectionManager().ActiveConnections()
			ci.DispatchService().Publish(invocation.NewGroupLost(conns[0].ConnectionID(), fmt.Errorf("foo error: %w", hzerrors.ErrIO)))
		}()
		// the invocation should be sent after 5 seconds, make sure the set operation below succeeds in 10 seconds or times out.
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err := m.Set(ctx, "foo", "bar")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func testListenersAfterClientDisconnected(t *testing.T, memberHost string, clientHost string, port int) {
	const heartBeatSec = 6
	// launch the cluster
	memberConfig := listenersAfterClientDisconnectedXMLConfig(t.Name(), memberHost, port, heartBeatSec)
	if it.SSLEnabled() {
		memberConfig = listenersAfterClientDisconnectedXMLSSLConfig(t.Name(), memberHost, port, heartBeatSec)
	}
	tc := it.StartNewClusterWithConfig(1, memberConfig, port)
	// create and start the client
	config := tc.DefaultConfig()
	config.Cluster.Network.SetAddresses(fmt.Sprintf("%s:%d", clientHost, port))
	if it.TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
	defer client.Shutdown(ctx)
	ec := int64(0)
	m := it.MustValue(client.GetMap(ctx, it.NewUniqueObjectName("map"))).(*hz.Map)
	lc := hz.MapEntryListenerConfig{}
	lc.NotifyEntryAdded(true)
	it.MustValue(m.AddEntryListener(ctx, lc, func(event *hz.EntryNotified) {
		atomic.AddInt64(&ec, 1)
	}))
	ci := hz.NewClientInternal(client)
	// make sure the client connected to the member
	it.Eventually(t, func() bool {
		ac := len(ci.ConnectionManager().ActiveConnections())
		t.Logf("active connections: %d", ac)
		return ac == 1
	})
	// shutdown the member
	tc.Shutdown()
	time.Sleep(2 * heartBeatSec * time.Second)
	// launch a member with the same address
	tc = it.StartNewClusterWithConfig(1, memberConfig, port)
	defer tc.Shutdown()
	// entry notified event handler should run
	it.Eventually(t, func() bool {
		it.MustValue(m.Remove(ctx, 1))
		it.MustValue(m.Put(ctx, 1, 2))
		return atomic.LoadInt64(&ec) > 0
	})
}

type invokeFilter func(inv invocation.Invocation) (ok bool)

type riggedInvocationHandler struct {
	invocation.Handler
	invokeFilter invokeFilter
}

func newRiggedInvocationHandler(handler invocation.Handler, filter invokeFilter) *riggedInvocationHandler {
	return &riggedInvocationHandler{Handler: handler, invokeFilter: filter}
}

func (h *riggedInvocationHandler) Invoke(inv invocation.Invocation) (int64, error) {
	if !h.invokeFilter(inv) {
		return 1, nil
	}
	return h.Handler.Invoke(inv)
}

func TestClusterID(t *testing.T) {
	it.SkipIf(t, "oss")
	clientTester(t, func(t *testing.T, smart bool) {
		ctx := context.Background()
		cls1 := it.StartNewClusterWithOptions("clusterId-test-cluster1", 15701, it.MemberCount())
		cls2 := it.StartNewClusterWithOptions("clusterId-test-cluster2", 16701, it.MemberCount())
		defer func() {
			cls2.Shutdown()
			cls1.Shutdown()
		}()
		var wg sync.WaitGroup
		wg.Add(1)
		config1 := cls1.DefaultConfig()
		config1.Cluster.ConnectionStrategy.Timeout = types.Duration(5 * time.Second)
		config2 := cls2.DefaultConfig()
		config := hz.Config{
			Failover: cluster.FailoverConfig{
				Enabled:  true,
				TryCount: 5,
			},
		}
		config.Failover.SetConfigs(config1.Cluster, config2.Cluster)
		config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
			if event.State == hz.LifecycleStateChangedCluster {
				wg.Done()
			}
		})
		c := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
		defer func(ctx context.Context, c *hz.Client) {
			err := c.Shutdown(ctx)
			if err != nil {
				t.Error("client should had shutdown properly")
			}
		}(ctx, c)
		ci := hz.NewClientInternal(c)
		prevClusterId := ci.ClusterID()
		cls1.Shutdown()
		assert.Equal(t, ci.ClusterID(), types.UUID{})
		wg.Wait()
		it.Eventually(t, func() bool {
			if !c.Running() {
				return false
			}
			currClusterId := ci.ClusterID()
			switch currClusterId {
			case types.UUID{}, prevClusterId:
				return false
			default:
				return true
			}
		})
	})
}
