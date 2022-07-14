//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"errors"
	"fmt"
	"reflect"
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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Tests that require the hazelcastinternal tag.

func TestListenersAfterClientDisconnected(t *testing.T) {
	t.Run("MemberHostname_ClientIP_AddEntryListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "127.0.0.1", 46501, addEntryListener)
	})
	t.Run("MemberHostname_ClientIP_AddListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "127.0.0.1", 46501, addListener)
	})

	t.Run("MemberHostname_ClientHostname_AddEntryListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "localhost", 47501, addEntryListener)
	})
	t.Run("MemberHostname_ClientHostname_AddListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "localhost", "localhost", 47501, addListener)
	})

	t.Run("MemberIP_ClientIP", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "127.0.0.1", 48501, addEntryListener)
	})
	t.Run("MemberIP_ClientIP_AddListener_AddEntryListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "127.0.0.1", 48501, addListener)
	})

	t.Run("MemberIP_ClientHostname_AddEntryListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "localhost", 49501, addEntryListener)
	})
	t.Run("MemberIP_ClientHostname_AddListener", func(t *testing.T) {
		testListenersAfterClientDisconnected(t, "127.0.0.1", "localhost", 49501, addListener)
	})
}

func TestNotReceivedInvocation(t *testing.T) {
	// This test skips sending an invocation to the member in order to simulate lost connection.
	// After 5 seconds, a GroupLost event is published to simulate the disconnection.
	clientTester(t, func(t *testing.T, smart bool) {
		tc := it.StartNewClusterWithOptions("not-received-invocation", 55741, 1)
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

func addEntryListener(ctx context.Context, m *hz.Map, counter *int64) {
	lc := hz.MapEntryListenerConfig{}
	lc.NotifyEntryAdded(true)
	it.MustValue(m.AddEntryListener(ctx, lc, func(event *hz.EntryNotified) {
		atomic.AddInt64(counter, 1)
	}))
}

func addListener(ctx context.Context, m *hz.Map, counter *int64) {
	it.MustValue(m.AddListener(ctx, hz.MapListener{
		EntryAdded: func(event *hz.EntryNotified) {
			atomic.AddInt64(counter, 1)
		},
	}, false))
}

func testListenersAfterClientDisconnected(t *testing.T, memberHost string, clientHost string, port int, f func(context.Context, *hz.Map, *int64)) {
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
	f(ctx, m, &ec)
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

func TestClusterID(t *testing.T) {
	t.Skipf("flaky, see: https://github.com/hazelcast/hazelcast-go-client/issues/844")
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
		c := ensureClient(config)
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

func TestClientInternal_ClusterID(t *testing.T) {
	tc := it.StartNewClusterWithOptions("ci-cluster-id", 55711, 1)
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, tc.DefaultConfig()))
	defer client.Shutdown(ctx)
	ci := hz.NewClientInternal(client)
	assert.NotEqual(t, types.UUID{}, ci.ClusterID())
	tc.Shutdown()
	assert.Equal(t, types.UUID{}, ci.ClusterID())
}

func TestClientInternal_OrderedMembers(t *testing.T) {
	t.Skipf("flaky test: https://github.com/hazelcast/hazelcast-go-client/issues/789")
	// start a 1 member cluster
	tc := it.StartNewClusterWithOptions("ci-orderedmembers", 55721, 1)
	defer tc.Shutdown()
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, tc.DefaultConfig()))
	defer client.Shutdown(ctx)
	ci := hz.NewClientInternal(client)
	targetUUIDs := tc.MemberUUIDs
	assert.True(t, sameMembers(targetUUIDs, ci.OrderedMembers()))
	// start another member
	mem, err := tc.RC.StartMember(ctx, tc.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	targetUUIDs = append(targetUUIDs, mem.UUID)
	assert.True(t, sameMembers(targetUUIDs, ci.OrderedMembers()))
	// start another member
	mem, err = tc.RC.StartMember(ctx, tc.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	targetUUIDs = append(targetUUIDs, mem.UUID)
	assert.True(t, sameMembers(targetUUIDs, ci.OrderedMembers()))
	// stop a member
	stopped := ci.OrderedMembers()[0]
	if _, err := tc.RC.ShutdownMember(ctx, tc.ClusterID, stopped.UUID.String()); err != nil {
		t.Fatal(err)
	}
	targetUUIDs = targetUUIDs[1:]
	it.Eventually(t, func() bool {
		return sameMembers(targetUUIDs, ci.OrderedMembers())
	})
}

func TestClientInternal_ConnectedToMember(t *testing.T) {
	tc := it.StartNewClusterWithOptions("ci-connected-to-member", 55731, 2)
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, tc.DefaultConfig()))
	defer client.Shutdown(ctx)
	ci := hz.NewClientInternal(client)
	tc.Shutdown()
	it.Eventually(t, func() bool {
		return len(filterConnectedMembers(ci)) == 0
	})
}

func TestClientInternal_InvokeOnRandomTarget(t *testing.T) {
	clientInternalTester(t, "ci-invoke-random", func(t *testing.T, ci *hz.ClientInternal) {
		ctx := context.Background()
		t.Run("without handler", func(t *testing.T) {
			req := EncodeMCGetMemberConfigRequest()
			resp, err := ci.InvokeOnRandomTarget(ctx, req, nil)
			if err != nil {
				t.Fatal(err)
			}
			s := DecodeMCGetMemberConfigResponse(resp)
			assert.Greater(t, len(s), 0)
		})
		t.Run("with handler", func(t *testing.T) {
			invoked := int32(0)
			opts := &hz.InvokeOptions{
				Handler: func(clientMessage *hz.ClientMessage) {
					atomic.StoreInt32(&invoked, 1)
				},
			}
			req := codec.EncodeMapAddEntryListenerRequest("foo", true, int32(hz.EntryAdded), false)
			if _, err := ci.InvokeOnRandomTarget(ctx, req, opts); err != nil {
				t.Fatal(err)
			}
			m, err := ci.Client().GetMap(ctx, "foo")
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Set(ctx, "key", "value"); err != nil {
				t.Fatal(err)
			}
			it.Eventually(t, func() bool {
				return atomic.LoadInt32(&invoked) == 1
			})
		})

	})
}

func TestClientInternal_InvokeOnPartition(t *testing.T) {
	clientInternalTester(t, "ci-invoke-partition", func(t *testing.T, ci *hz.ClientInternal) {
		req := EncodeMCGetMemberConfigRequest()
		resp, err := ci.InvokeOnPartition(context.Background(), req, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		s := DecodeMCGetMemberConfigResponse(resp)
		assert.Greater(t, len(s), 0)
	})
}

func TestClientInternal_InvokeOnKey(t *testing.T) {
	clientInternalTester(t, "ci-invoke-key", func(t *testing.T, ci *hz.ClientInternal) {
		keyData, err := ci.EncodeData("foo")
		if err != nil {
			t.Fatal(err)
		}
		req := EncodeMCGetMemberConfigRequest()
		resp, err := ci.InvokeOnKey(context.Background(), req, keyData, nil)
		if err != nil {
			t.Fatal(err)
		}
		s := DecodeMCGetMemberConfigResponse(resp)
		assert.Greater(t, len(s), 0)
	})
}

func TestClientInternal_InvokeOnMember(t *testing.T) {
	clientInternalTester(t, "ci-invoke-member", func(t *testing.T, ci *hz.ClientInternal) {
		ctx := context.Background()
		t.Run("invalid member", func(t *testing.T) {
			_, err := ci.InvokeOnMember(ctx, nil, types.UUID{}, nil)
			if !errors.Is(err, hzerrors.ErrIllegalArgument) {
				t.Fatalf("expected hzerrors.ErrIllegalArgument but received: %v", err)
			}
		})
		t.Run("valid member", func(t *testing.T) {
			mem := ci.ClusterService().OrderedMembers()[0]
			req := EncodeMCGetMemberConfigRequest()
			resp, err := ci.InvokeOnMember(ctx, req, mem.UUID, nil)
			if err != nil {
				t.Fatal(err)
			}
			s := DecodeMCGetMemberConfigResponse(resp)
			assert.Greater(t, len(s), 0)
		})
	})
}

func TestClientInternal_EncodeData(t *testing.T) {
	clientInternalTester(t, "ci-encode-data", func(t *testing.T, ci *hz.ClientInternal) {
		data, err := ci.EncodeData("foo")
		if err != nil {
			t.Fatal(err)
		}
		v, err := ci.DecodeData(data)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "foo", v)
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

func clientInternalTester(t *testing.T, clusterName string, f func(t *testing.T, ci *hz.ClientInternal)) {
	tc := it.StartNewClusterWithOptions(clusterName, 55751, 1)
	defer tc.Shutdown()
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, tc.DefaultConfig()))
	defer client.Shutdown(ctx)
	ci := hz.NewClientInternal(client)
	f(t, ci)
}

func sameMembers(target []string, mems []cluster.MemberInfo) bool {
	memUUIDs := make([]string, len(mems))
	for i, mem := range mems {
		memUUIDs[i] = mem.UUID.String()
	}
	return reflect.DeepEqual(target, memUUIDs)
}

func filterConnectedMembers(ci *hz.ClientInternal) []cluster.MemberInfo {
	mems := ci.OrderedMembers()
	var connected []cluster.MemberInfo
	for _, mem := range mems {
		if ci.ConnectedToMember(mem.UUID) {
			connected = append(connected, mem)
		}
	}
	return connected
}

const (
	MCGetMemberConfigCodecRequestMessageType  = int32(0x200500)
	MCGetMemberConfigCodecResponseMessageType = int32(0x200501)

	MCGetMemberConfigCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Gets the effective config of a member rendered as XML.

func EncodeMCGetMemberConfigRequest() *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MCGetMemberConfigCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MCGetMemberConfigCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeMCGetMemberConfigResponse(clientMessage *proto.ClientMessage) string {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return codec.DecodeString(frameIterator)
}

// ensureClient prevents client start to fail the test when the client is not allowed in the cluster.
func ensureClient(config hz.Config) *hz.Client {
	for i := 0; i < 60; i++ {
		client, err := hz.StartNewClientWithConfig(context.Background(), config)
		if err != nil {
			if errors.Is(err, hzerrors.ErrClientNotAllowedInCluster) {
				time.Sleep(1 * time.Second)
				continue
			}
			panic(err)
		}
		return client
	}
	panic("the client could not connect to the cluster in 60 seconds.")
}
