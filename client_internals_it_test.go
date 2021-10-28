// +build hazelcastinternal

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
	"sync/atomic"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/logger"
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

func testListenersAfterClientDisconnected(t *testing.T, memberHost string, clientHost string, port int) {
	const heartBeatSec = 6
	// launch the cluster
	memberConfig := listenersAfterClientDisconnectedXMLConfig(t.Name(), memberHost, port, heartBeatSec)
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
	ci := hz.NewClientInternals(client)
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
