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

package hazelcast_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestClient(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "AddDistributedObjectListener", f: clientAddDistributedObjectListenerTest},
		{name: "AddLifecycleListener", f: clientAddLifecycleListenerTest},
		{name: "AddMembershipListener", f: clientAddMembershipListenerTest},
		{name: "ClusterReconnectionReconnectModeOff", f: clientClusterReconnectionReconnectModeOffTest},
		{name: "ClusterReconnectionShutdownCluster", f: clientClusterReconnectionShutdownClusterTest},
		{name: "ClusterShutdownThenCheckOperationsNotHanging", f: clientClusterShutdownThenCheckOperationsNotHangingTest},
		{name: "ClusterConnectionToMultipleAddrs", f: clusterConnectionToMultipleAddrsTest},
		{name: "EventHandlingOrder", f: clientEventHandlingOrderTest},
		{name: "EventOrder", f: clientEventOrderTest},
		{name: "FailoverEECluster", f: clientFailoverEEClusterTest},
		{name: "FailoverEEClusterReconnection", f: clientFailoverEEClusterReconnectionTest},
		{name: "FailoverOSSCluster", f: clientFailoverOSSClusterTest},
		{name: "FixConnection", f: clientFixConnectionTest},
		{name: "GetDistributedObjects", f: clientGetDistributedObjectsTest},
		{name: "GetProxyInstance", f: clientGetProxyInstanceTest},
		{name: "Heartbeat", f: clientHeartbeatTest},
		{name: "InvocationAfterShutdown", f: clientInvocationAfterShutdownTest},
		{name: "InvocationTimeout", f: clientInvocationTimeoutTest},
		{name: "LifecycleEvents", f: clientLifecycleEventsTest},
		{name: "MemberEvents", f: clientMemberEventsTest},
		{name: "Name", f: clientNameTest},
		{name: "PortRangeAllAddresses", f: clientPortRangeAllAddressesTest},
		{name: "PortRangeMultipleAddresses", f: clientPortRangeMultipleAddressesTest},
		{name: "PortRangeSingleAddress", f: clientPortRangeSingleAddressTest},
		{name: "RemoveLifecycleListener", f: clientRemoveLifecycleListenerTest},
		{name: "RemoveMembershipListener", f: clientRemoveMembershipListenerTest},
		{name: "Running", f: clientRunningTest},
		{name: "Shutdown", f: clientShutdownTest},
		{name: "ShutdownRace", f: clientShutdownRaceTest},
		{name: "StartShutdownMemoryLeak", f: clientStartShutdownMemoryLeakTest},
		{name: "StartShutdownWithNilContext", f: clientStartShutdownWithNilContextTest},
		{name: "Version", f: clientVersionTest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func clientLifecycleEventsTest(t *testing.T) {
	t.Parallel()
	receivedStates := []hz.LifecycleState{}
	receivedStatesMu := &sync.RWMutex{}
	configCallback := func(config *hz.Config) {
		config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
			receivedStatesMu.Lock()
			defer receivedStatesMu.Unlock()
			switch event.State {
			case hz.LifecycleStateStarting:
				t.Logf("Received starting state")
			case hz.LifecycleStateStarted:
				t.Logf("Received started state")
			case hz.LifecycleStateShuttingDown:
				t.Logf("Received shutting down state")
			case hz.LifecycleStateShutDown:
				t.Logf("Received shut down state")
			case hz.LifecycleStateConnected:
				t.Logf("Received client connected state")
			case hz.LifecycleStateDisconnected:
				t.Logf("Received client disconnected state")
			case hz.LifecycleStateChangedCluster:
				t.Logf("Connected cluster has been changed")
			default:
				t.Log("Received unknown state:", event.State)
			}
			receivedStates = append(receivedStates, event.State)
		})
	}
	it.TesterWithConfigBuilder(t, configCallback, func(t *testing.T, client *hz.Client) {
		defer func() {
			receivedStatesMu.Lock()
			receivedStates = []hz.LifecycleState{}
			receivedStatesMu.Unlock()
		}()
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		targetStates := []hz.LifecycleState{
			hz.LifecycleStateStarting,
			hz.LifecycleStateConnected,
			hz.LifecycleStateStarted,
			hz.LifecycleStateShuttingDown,
			hz.LifecycleStateShutDown,
		}
		it.Eventually(t, func() bool {
			receivedStatesMu.RLock()
			defer receivedStatesMu.RUnlock()
			return reflect.DeepEqual(targetStates, receivedStates)
		})
	})
}

func clientAddLifecycleListenerTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var receivedStates []hz.LifecycleState
		receivedStatesMu := &sync.RWMutex{}
		subscriptionID, err := client.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
			receivedStatesMu.Lock()
			defer receivedStatesMu.Unlock()
			// client is already connected
			switch event.State {
			case hz.LifecycleStateShuttingDown:
				t.Logf("Received shutting down state")
			case hz.LifecycleStateShutDown:
				t.Logf("Received shut down state")
			default:
				t.Log("Received unknown state:", event.State)
			}
			receivedStates = append(receivedStates, event.State)
		})
		require.Nil(t, err)
		require.NotEqual(t, types.UUID{}, subscriptionID, "subscription UUID should not be empty")
		if err = client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		targetStates := []hz.LifecycleState{
			hz.LifecycleStateShuttingDown,
			hz.LifecycleStateShutDown,
		}
		it.Eventually(t, func() bool {
			receivedStatesMu.RLock()
			defer receivedStatesMu.RUnlock()
			return reflect.DeepEqual(targetStates, receivedStates)
		})
	})
}

func clientRemoveLifecycleListenerTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var lifecycleEventReceived int32
		subscriptionID, err := client.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
			// shutting down event is published before the actual shutdown is occurred
			// client must not be shutdown before that event was published
			if event.State == hz.LifecycleStateShuttingDown {
				atomic.AddInt32(&lifecycleEventReceived, 1)
			}
		})
		require.Nil(t, err)
		require.NotEqual(t, types.UUID{}, subscriptionID, "subscription UUID should not be empty")
		if err = client.RemoveLifecycleListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		if err = client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool {
			return !client.Running() && lifecycleEventReceived == 0
		})
	})
}

func clientAddMembershipListenerTest(t *testing.T) {
	t.Parallel()
	var (
		wgAdded,
		wgRemoved sync.WaitGroup
		memberCount int = 3
	)
	wgAdded.Add(1)
	wgRemoved.Add(1)
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), memberCount)
	defer cls.Shutdown()
	client, err := hz.StartNewClientWithConfig(ctx, cls.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func(ctx context.Context, client *hz.Client) {
		err = client.Shutdown(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}(ctx, client)
	subscriptionID, err := client.AddMembershipListener(func(event cluster.MembershipStateChanged) {
		switch event.State {
		case cluster.MembershipStateAdded:
			t.Log("MembershipStateAdded")
			wgAdded.Done()
		case cluster.MembershipStateRemoved:
			t.Log("MembershipStateRemoved")
			wgRemoved.Done()
		}
	})
	require.Nil(t, err)
	require.NotEqual(t, types.UUID{}, subscriptionID, "subscription UUID should not be empty")
	// member which is not connected by the client initially.
	uuid := cls.MemberUUIDs[1]
	//uuid := cls.MemberUUIDs[0]
	ok, err := cls.RC.TerminateMember(ctx, cls.ClusterID, uuid)
	it.Must(err)
	require.True(t, ok, "rc cannot terminate member")
	wgRemoved.Wait()
	_, err = cls.RC.StartMember(ctx, cls.ClusterID)
	it.Must(err)
	wgAdded.Wait()
}

func clientRemoveMembershipListenerTest(t *testing.T) {
	t.Parallel()
	var removed int32
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), 2)
	defer cls.Shutdown()
	cfg := cls.DefaultConfig()
	client, err := hz.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ctx context.Context, client *hz.Client) {
		err = client.Shutdown(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}(ctx, client)
	subscriptionID, err := client.AddMembershipListener(func(event cluster.MembershipStateChanged) {
		switch event.State {
		case cluster.MembershipStateRemoved:
			t.Log("MembershipStateRemoved")
			atomic.AddInt32(&removed, 1)
		}
	})
	require.Nil(t, err)
	require.NotEqual(t, types.UUID{}, subscriptionID, "subscription UUID should not be empty")
	err = client.RemoveMembershipListener(subscriptionID)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := cls.RC.TerminateMember(ctx, cls.ClusterID, cls.MemberUUIDs[1])
	it.Must(err)
	require.True(t, ok, "rc cannot terminate member")
	client.Shutdown(ctx)
	it.Eventually(t, func() bool {
		return !client.Running() && removed == 0
	})
}

func clientRunningTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		assert.True(t, client.Running())
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		assert.False(t, client.Running())
	})
}

func clientNameTest(t *testing.T) {
	t.Parallel()
	clientName := "test-client-name"
	cb := func(config *hz.Config) {
		config.ClientName = clientName
	}
	it.TesterWithConfigBuilder(t, cb, func(t *testing.T, client *hz.Client) {
		assert.Equal(t, clientName, client.Name())
	})
}

func clientPortRangeAllAddressesTest(t *testing.T) {
	t.Parallel()
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{"127.0.0.1", "localhost", "0.0.0.0"}
	}, 4, 3)
}

func clientPortRangeMultipleAddressesTest(t *testing.T) {
	t.Parallel()
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{"127.0.0.1", "localhost", fmt.Sprintf("0.0.0.0:%d", validPort)}
	}, 4, 3)
}

func clientPortRangeSingleAddressTest(t *testing.T) {
	t.Parallel()
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{fmt.Sprintf("localhost:%d", validPort), "127.0.0.1", fmt.Sprintf("0.0.0.0:%d", validPort)}
	}, 4, 3)
}

func clientMemberEventsTest(t *testing.T) {
	t.Parallel()
	handlerCalled := int32(0)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	configCallback := func(config *hz.Config) {
		config.AddMembershipListener(func(event cluster.MembershipStateChanged) {
			if atomic.CompareAndSwapInt32(&handlerCalled, 0, 1) {
				wg.Done()
			}
		})
	}
	it.TesterWithConfigBuilder(t, configCallback, func(t *testing.T, client *hz.Client) {
		defer func() {
			atomic.StoreInt32(&handlerCalled, 0)
			wg.Add(1)
		}()
		wg.Wait()
	})
}

func clientEventOrderTest(t *testing.T) {
	t.Parallel()
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		// events should be processed in this order
		const (
			noPrevEvent = 0
			addEvent    = 1
			removeEvent = 2
		)
		// populate event order checkers
		var checkers []*int32
		for i := 0; i < 20; i++ {
			var state int32
			checkers = append(checkers, &state)
		}
		// init listener conf
		var c hz.MapEntryListenerConfig
		c.NotifyEntryAdded(true)
		c.NotifyEntryRemoved(true)
		var tasks sync.WaitGroup
		// add and remove are separate tasks
		tasks.Add(len(checkers) * 2)
		it.MustValue(m.AddEntryListener(ctx, c, func(e *hz.EntryNotified) {
			state := checkers[e.Key.(int64)]
			switch e.EventType {
			case hz.EntryAdded:
				if !atomic.CompareAndSwapInt32(state, noPrevEvent, noPrevEvent) {
					panic("order is not preserved")
				}
				// keep the executor busy, make sure remove event is not processed before this
				time.Sleep(500 * time.Millisecond)
				if !atomic.CompareAndSwapInt32(state, noPrevEvent, addEvent) {
					panic("order is not preserved")
				}
				tasks.Done()
			case hz.EntryRemoved:
				if !atomic.CompareAndSwapInt32(state, addEvent, removeEvent) {
					panic("order is not preserved")
				}
				tasks.Done()
			}
		}))
		for i := range checkers {
			tmp := i
			go func(index int) {
				it.MustValue(m.Put(ctx, index, "test"))
				it.MustValue(m.Remove(ctx, index))
			}(tmp)
		}
		tasks.Wait()
	})
}

func clientEventHandlingOrderTest(t *testing.T) {
	t.Parallel()
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		var lc hz.MapEntryListenerConfig
		lc.IncludeValue = true
		lc.NotifyEntryAdded(true)
		lc.NotifyEntryRemoved(true)
		const iterationCount = 5000
		const eventCount = iterationCount * 2
		var (
			// event journal to keep track of order of the published events
			journal = make([]*hz.EntryNotified, 0, eventCount)
			// wait for all events to be processed
			wg  sync.WaitGroup
			mut sync.Mutex
		)
		wg.Add(eventCount)
		handler := func(event *hz.EntryNotified) {
			mut.Lock()
			journal = append(journal, event)
			mut.Unlock()
			wg.Done()
		}
		it.MustValue(m.AddEntryListener(ctx, lc, handler))
		for i := 0; i < iterationCount; i++ {
			it.Must(m.Set(ctx, "sameKey", i))
			it.MustValue(m.Remove(ctx, "sameKey"))
		}
		wg.Wait()
		assert.Equal(t, eventCount, len(journal))
		for i := 0; i < eventCount; i += 2 {
			assert.Equal(t, hz.EntryAdded, journal[i].EventType)
			assert.Equal(t, hz.EntryRemoved, journal[i+1].EventType)
			v := i / 2
			assert.Equal(t, int64(v), journal[i].Value)
			assert.Equal(t, int64(v), journal[i+1].OldValue)
		}
	})
}

func clientHeartbeatTest(t *testing.T) {
	t.Parallel()
	it.MarkSlow(t)
	it.MapTesterWithConfig(t, func(config *hz.Config) {
	}, func(t *testing.T, m *hz.Map) {
		time.Sleep(150 * time.Second)
		target := "v1"
		it.Must(m.Set(context.Background(), "k1", target))
		if v := it.MustValue(m.Get(context.Background(), "k1")); target != v {
			t.Fatalf("target: %v != %v", target, v)
		}
	})
}

func clientShutdownTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutting down second time should not return an error")
		}
	})
}

func clientShutdownRaceTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		const goroutineCount = 100
		wg := &sync.WaitGroup{}
		wg.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func() {
				defer wg.Done()
				client.Shutdown(context.Background())
			}()
		}
		wg.Wait()
	})
}

func clientAddDistributedObjectListenerTest(t *testing.T) {
	// TODO: Adapt this test for t.Parallel()
	//t.Parallel()
	type objInfo struct {
		service string
		object  string
		count   int
	}
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var created, destroyed objInfo
		mu := &sync.Mutex{}
		handler := func(e hz.DistributedObjectNotified) {
			mu.Lock()
			defer mu.Unlock()
			switch e.EventType {
			case hz.DistributedObjectCreated:
				created.service = e.ServiceName
				created.object = e.ObjectName
				created.count++
			case hz.DistributedObjectDestroyed:
				destroyed.service = e.ServiceName
				destroyed.object = e.ObjectName
				destroyed.count++
			}
		}
		subID, err := client.AddDistributedObjectListener(context.Background(), handler)
		if err != nil {
			t.Fatal(err)
		}
		name := it.NewUniqueObjectName("dolistener-tester")
		m := it.MustValue(client.GetMap(context.Background(), name)).(*hz.Map)
		it.Must(m.Destroy(context.Background()))
		targetObjInfo := objInfo{service: hz.ServiceNameMap, object: name, count: 1}
		it.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			if targetObjInfo != created {
				return false
			}
			return targetObjInfo == destroyed
		})
		if err = client.RemoveDistributedObjectListener(context.Background(), subID); err != nil {
			t.Fatal(err)
		}
		m = it.MustValue(client.GetMap(context.Background(), name)).(*hz.Map)
		it.Must(m.Destroy(context.Background()))
		it.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			log.Printf("targetObj: %v, created: %v, destroyed: %v", targetObjInfo, created, destroyed)
			if targetObjInfo != created {
				return false
			}
			return targetObjInfo == destroyed
		})
	})
}

func clientClusterReconnectionShutdownClusterTest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	port := it.NextPort()
	cls := it.StartNewClusterWithOptions(t.Name(), port, it.MemberCount())
	mu := &sync.Mutex{}
	events := []hz.LifecycleState{}
	config := cls.DefaultConfig()
	disconnectedWg := sync.WaitGroup{}
	disconnectedWg.Add(1)
	reconnectedWg := sync.WaitGroup{}
	reconnectedWg.Add(1)
	connectedCount := 0
	disconnectedCount := 0
	config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
		mu.Lock()
		if event.State == hz.LifecycleStateDisconnected {
			disconnectedCount++
			if disconnectedCount == 1 {
				disconnectedWg.Done()
			}
		}
		if event.State == hz.LifecycleStateConnected {
			connectedCount++
			if connectedCount == 2 {
				reconnectedWg.Done()
			}
		}
		events = append(events, event.State)
		mu.Unlock()
	})
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	cls.Shutdown()
	it.WaitEventually(t, &disconnectedWg)
	cls = it.StartNewClusterWithOptions(t.Name(), port, it.MemberCount())
	it.WaitEventually(t, &reconnectedWg)
	cls.Shutdown()
	c.Shutdown(ctx)
	target := []hz.LifecycleState{
		hz.LifecycleStateStarting,
		hz.LifecycleStateConnected,
		hz.LifecycleStateStarted,
		hz.LifecycleStateDisconnected,
		hz.LifecycleStateChangedCluster,
		hz.LifecycleStateConnected,
		hz.LifecycleStateDisconnected,
		hz.LifecycleStateShuttingDown,
		hz.LifecycleStateShutDown,
	}
	it.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return reflect.DeepEqual(target, events)
	})
}

func clientClusterReconnectionReconnectModeOffTest(t *testing.T) {
	// TODO: Adapt this test for t.Parallel()
	//t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cls := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), it.MemberCount())
	config := cls.DefaultConfig()
	config.Cluster.ConnectionStrategy.ReconnectMode = cluster.ReconnectModeOff
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	cls.Shutdown()
	assert.Equal(t, false, c.Running())
}

// portRangeConnectivityTest sets up a port range where we can make sure,that our port will be in the range
// and we can test the connectivity try
func portRangeConnectivityTest(t *testing.T, getAddresses func(validPort int) []string, portsToTryBefore int, portsToTryAfter int) {
	it.TesterWithConfigBuilder(t, func(config *hz.Config) {
		clusterAddress := config.Cluster.Network.Addresses[0]
		_, port, err := internal.ParseAddr(clusterAddress)
		if err != nil {
			t.Fatal(err)
		}
		config.Cluster.Network.SetAddresses(getAddresses(port)...)
		config.Cluster.Network.SetPortRange(port-portsToTryBefore, port+portsToTryAfter)
	},
		func(t *testing.T, client *hz.Client) {
			assert.True(t, client.Running())
			if err := client.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			assert.False(t, client.Running())
		})
}

func clientGetDistributedObjectsTest(t *testing.T) {
	t.Parallel()
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var (
			testMapName = it.NewUniqueObjectName("map")
			testSetName = it.NewUniqueObjectName("set")
			mapInfo     = types.DistributedObjectInfo{Name: testMapName, ServiceName: hz.ServiceNameMap}
			setInfo     = types.DistributedObjectInfo{Name: testSetName, ServiceName: hz.ServiceNameSet}
		)
		ctx := context.Background()
		testMap, err := client.GetMap(ctx, testMapName)
		if err != nil {
			t.Fatal(err)
		}
		testSet, err := client.GetSet(ctx, testSetName)
		if err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool {
			objects, err := client.GetDistributedObjectsInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return containsDistributedObject(objects, mapInfo) && containsDistributedObject(objects, setInfo)
		})
		if err = testMap.Destroy(ctx); err != nil {
			t.Fatal(err)
		}
		if err = testSet.Destroy(ctx); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool {
			objects, err := client.GetDistributedObjectsInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return !containsDistributedObject(objects, mapInfo) && !containsDistributedObject(objects, setInfo)
		})
	})
}

func clientGetProxyInstanceTest(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		getFn func(ctx context.Context, client *hz.Client, name string) (interface{}, error)
		name  string
	}{
		{
			name: "map",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetMap(ctx, name)
			},
		},
		{
			name: "replicated-map",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetReplicatedMap(ctx, name)
			},
		},
		{
			name: "multi-map",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetMultiMap(ctx, name)
			},
		},
		{
			name: "list",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetList(ctx, name)
			},
		},
		{
			name: "queue",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetQueue(ctx, name)
			},
		},
		{
			name: "topic",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetTopic(ctx, name)
			},
		},
		{
			name: "set",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetSet(ctx, name)
			},
		},
		{
			name: "pn-counter",
			getFn: func(ctx context.Context, client *hz.Client, name string) (interface{}, error) {
				return client.GetPNCounter(ctx, name)
			},
		},
	}
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dsName := it.NewUniqueObjectName(tc.name)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				p1, err := tc.getFn(ctx, client, dsName)
				if err != nil {
					t.Fatal(err)
				}
				p2, err := tc.getFn(ctx, client, dsName)
				if err != nil {
					t.Fatal(err)
				}
				assert.Same(t, p1, p2, "same proxy struct instances expected")
			})
		}
	})
}

func clientFailoverOSSClusterTest(t *testing.T) {
	t.Parallel()
	skip.IfNot(t, "oss")
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), it.MemberCount())
	defer cls.Shutdown()
	config := cls.DefaultConfig()
	config.Failover.Enabled = true
	config.Failover.TryCount = 1
	failoverConfig := config.Cluster
	failoverConfig.Name = "backup-failover-test-cluster"
	config.Failover.SetConfigs(failoverConfig)
	_, err := hz.StartNewClientWithConfig(ctx, config)
	if !errors.Is(err, hzerrors.ErrIllegalState) {
		t.Fatalf("should have returned a client illegal state error")
	}
}

func clientFailoverEEClusterTest(t *testing.T) {
	t.Parallel()
	skip.IfNot(t, "enterprise")
	ctx := context.Background()
	clsBase := t.Name()
	cls1 := it.StartNewClusterWithOptions(fmt.Sprintf("%s-1", clsBase), it.NextPort(), it.MemberCount())
	defer cls1.Shutdown()
	cls2 := it.StartNewClusterWithOptions(fmt.Sprintf("%s-2", clsBase), it.NextPort(), it.MemberCount())
	defer cls2.Shutdown()
	config := hz.Config{}
	if it.TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	config.Failover.Enabled = true
	cfg1 := cls1.DefaultConfig()
	cfg1.Cluster.Name = "not-this-cluster's-name"
	cfg1.Cluster.ConnectionStrategy.Timeout = types.Duration(10 * time.Second)
	cfg2 := cls2.DefaultConfig()
	config.Failover.SetConfigs(cfg1.Cluster, cfg2.Cluster)
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("should have connected to failover cluster")
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func clientFailoverEEClusterReconnectionTest(t *testing.T) {
	t.Parallel()
	skip.IfNot(t, "enterprise")
	ctx := context.Background()
	cls1Name := fmt.Sprintf("%s-1", t.Name())
	cls2Name := fmt.Sprintf("%s-2", t.Name())
	cls1 := it.StartNewClusterWithOptions(cls1Name, it.NextPort(), it.MemberCount())
	cls2 := it.StartNewClusterWithOptions(cls2Name, it.NextPort(), it.MemberCount())
	defer cls2.Shutdown()
	var wg sync.WaitGroup
	wg.Add(1)
	config1 := cls1.DefaultConfig()
	config1.Cluster.ConnectionStrategy.Timeout = types.Duration(5 * time.Second)
	config2 := cls2.DefaultConfig()
	config := hz.Config{}
	if it.TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	config.Failover.Enabled = true
	config.Failover.TryCount = 10
	config.Failover.SetConfigs(config1.Cluster, config2.Cluster)
	config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
		if event.State == hz.LifecycleStateChangedCluster {
			wg.Done()
		}
	})
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	// shut down the first cluster
	cls1.Shutdown()
	// the client should reconnect to the second cluster
	wg.Wait()
	assert.True(t, c.Running())
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func clientFixConnectionTest(t *testing.T) {
	t.Parallel()
	// This test removes the member that corresponds to the connections which receives membership state changes.
	// Once that connection is closed, another connection should be randomly selected to receive membership state changes.
	// A new member is added to confirm that is the case.
	const memberCount = 3
	highlight := func(format string, args ...interface{}) {
		t.Logf("\n===\n%s\n===", fmt.Sprintf(format, args...))
	}
	addedCount := int64(0)
	ctx := context.Background()
	clusterName := t.Name()
	t.Log("Cluster name:", clusterName)
	port := it.NextPort()
	cls := it.StartNewClusterWithOptions(clusterName, port, memberCount)
	defer cls.Shutdown()
	config := cls.DefaultConfig()
	config.Cluster.Network.SetAddresses(fmt.Sprintf("localhost:%d", port+1))
	config.Cluster.Name = clusterName
	config.AddMembershipListener(func(event cluster.MembershipStateChanged) {
		highlight("%s member: %s", event.State.String(), event.Member.UUID)
		if event.State == cluster.MembershipStateAdded {
			atomic.AddInt64(&addedCount, 1)
		}
	})
	if it.TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	client, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(ctx)
	// terminate the member that corresponds to the connection which receives cluster membership updates
	mUUID := cls.MemberUUIDs[1]
	highlight("Terminated member: %s", mUUID)
	ok, err := cls.RC.TerminateMember(ctx, cls.ClusterID, mUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("could not terminate member: %s", err.Error())
	}
	m, err := cls.RC.StartMember(ctx, cls.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	highlight("Started member: %s", m.UUID)
	it.Eventually(t, func() bool {
		return int64(memberCount+1) == atomic.LoadInt64(&addedCount)
	})

}

func clientVersionTest(t *testing.T) {
	t.Parallel()
	// adding this test here, so there's no "unused lint warning.
	assert.Equal(t, "1.4.1", hz.ClientVersion)
}

func clientInvocationTimeoutTest(t *testing.T) {
	t.Parallel()
	clientTester(t, func(t *testing.T, smart bool) {
		tc := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), 1)
		defer tc.Shutdown()
		config := tc.DefaultConfig()
		if it.TraceLoggingEnabled() {
			config.Logger.Level = logger.TraceLevel
		}
		config.Cluster.InvocationTimeout = types.Duration(5 * time.Second)
		ctx := context.Background()
		client, err := hz.StartNewClientWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Shutdown(ctx)
		myMap, err := client.GetMap(ctx, "my-map")
		if err != nil {
			t.Fatal(err)
		}
		tc.Shutdown()
		it.Eventually(t, func() bool {
			_, err = myMap.Get(ctx, "k1")
			return err != nil
		})
	})
}

func clientStartShutdownMemoryLeakTest(t *testing.T) {
	t.Parallel()
	// TODO make sure there is no leak, and find an upper memory limit for this
	it.MarkFlaky(t)
	clientTester(t, func(t *testing.T, smart bool) {
		tc := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), it.MemberCount())
		defer tc.Shutdown()
		config := tc.DefaultConfig()
		if it.TraceLoggingEnabled() {
			config.Logger.Level = logger.TraceLevel
		}
		config.Cluster.Unisocket = !smart
		ctx := context.Background()
		var max uint64
		var m runtime.MemStats
		const limit = 8 * 1024 * 1024 // 8 MB
		runtime.GC()
		runtime.ReadMemStats(&m)
		base := m.Alloc
		for i := 0; i < 1000; i++ {
			client, err := hz.StartNewClientWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&m)
			t.Logf("memory allocation: %d at iteration: %d", m.Alloc, i)
			if m.Alloc > base && m.Alloc-base > limit {
				max = m.Alloc - base
				t.Fatalf("memory allocation: %d > %d (base: %d) at iteration: %d", max, limit, base, i)
			}
		}
	})
}

func clientInvocationAfterShutdownTest(t *testing.T) {
	t.Parallel()
	clientTester(t, func(t *testing.T, smart bool) {
		tc := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), it.MemberCount())
		defer tc.Shutdown()
		config := tc.DefaultConfig()
		if it.TraceLoggingEnabled() {
			config.Logger.Level = logger.TraceLevel
		}
		ctx := context.Background()
		client := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
		m, err := client.GetMap(ctx, "my-map")
		if err != nil {
			t.Fatal(err)
		}
		it.Must(client.Shutdown(ctx))
		_, err = m.Get(ctx, "foo")
		if !errors.Is(err, hzerrors.ErrClientNotActive) {
			t.Fatalf("expected hzerrors.ErrClientNotActive but received: %s", err.Error())
		}
	})
}

func clientClusterShutdownThenCheckOperationsNotHangingTest(t *testing.T) {
	t.Parallel()
	clientTester(t, func(t *testing.T, smart bool) {
		cn := fmt.Sprintf("%s-%t", t.Name(), smart)
		tc := it.StartNewClusterWithOptions(cn, it.NextPort(), it.MemberCount())
		defer tc.Shutdown()
		config := tc.DefaultConfig()
		cc := &config.Cluster
		cc.InvocationTimeout = types.Duration(24 * time.Hour)
		cc.RedoOperation = true
		cc.ConnectionStrategy.Timeout = types.Duration(20 * time.Second)
		if it.TraceLoggingEnabled() {
			config.Logger.Level = logger.TraceLevel
		}
		config.Cluster.Unisocket = !smart
		ctx := context.Background()
		client := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
		m, err := client.GetMap(ctx, it.NewUniqueObjectName("my-map"))
		if err != nil {
			t.Fatal(err)
		}
		const mapSize = 1000
		const gc = 100 // goroutine count
		wg := &sync.WaitGroup{}
		wg.Add(gc)
		startWg := &sync.WaitGroup{}
		startWg.Add(1)
		o := &sync.Once{}
		for i := 0; i < gc; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < mapSize; j++ {
					if j == mapSize/4 {
						o.Do(func() {
							startWg.Done()
						})
					}
					// ignoring the error below, it's not relevant
					_, _ = m.Put(ctx, j, j)
				}
			}(i)
		}
		it.WaitEventually(t, startWg)
		it.Must(client.Shutdown(ctx))
		it.WaitEventually(t, wg)
	})
}

func clientStartShutdownWithNilContextTest(t *testing.T) {
	t.Parallel()
	tc := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), 1)
	defer tc.Shutdown()
	client := it.MustClient(hz.StartNewClientWithConfig(nil, tc.DefaultConfig()))
	it.Must(client.Shutdown(nil))
}

func clusterConnectionToMultipleAddrsTest(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	tc := it.StartNewClusterWithOptions(t.Name(), it.NextPort(), 1)
	defer tc.Shutdown()
	mem, err := tc.RC.StartMember(ctx, tc.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	tc.MemberUUIDs = append(tc.MemberUUIDs, mem.UUID)
	config := tc.DefaultConfig()
	testCases := []struct {
		name      string
		addresses []string
		expectErr bool
	}{
		{
			name: "two valid seed addresses",
			addresses: []string{
				fmt.Sprintf("localhost:%d", tc.Port),
				fmt.Sprintf("localhost:%d", mem.Port),
			},
		},
		{
			name: "first addr valid, second is invalid",
			addresses: []string{
				fmt.Sprintf("localhost:%d", tc.Port),
				"non-existent.non-existent:1234",
			},
		},
		{
			name: "all addresses are invalid",
			addresses: []string{
				"non-existent.non-existent:123",
				"non-existent.non-existent:1234",
			},
			expectErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			configCopy := config
			configCopy.Cluster.Network.SetAddresses(tc.addresses...)
			client, err := hz.StartNewClientWithConfig(ctx, configCopy)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			it.Must(client.Shutdown(nil))
		})
	}
}

func clientTester(t *testing.T, f func(*testing.T, bool)) {
	if it.SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			f(t, true)
		})
	}
	if it.NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			f(t, false)
		})
	}
}

func containsDistributedObject(where []types.DistributedObjectInfo, what types.DistributedObjectInfo) bool {
	for _, o := range where {
		if o == what {
			return true
		}
	}
	return false
}

func listenersAfterClientDisconnectedXMLConfig(clusterName, publicAddr string, port, heartBeatSec int) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <network>
				<public-address>%s</public-address>
				<port>%d</port>
            </network>
			<properties>
				<property name="hazelcast.heartbeat.interval.seconds">%d</property>
			</properties>
        </hazelcast>
	`, clusterName, publicAddr, port, heartBeatSec)
}

func listenersAfterClientDisconnectedXMLSSLConfig(clusterName, publicAddr string, port, heartBeatSec int) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <network>
				<public-address>%s</public-address>
				<port>%d</port>
				<ssl enabled="true">
					<factory-class-name>
						com.hazelcast.nio.ssl.ClasspathSSLContextFactory
					</factory-class-name>
					<properties>
						<property name="keyStore">com/hazelcast/nio/ssl-mutual-auth/server1.keystore</property>
						<property name="keyStorePassword">password</property>
						<property name="keyManagerAlgorithm">SunX509</property>
						<property name="protocol">TLSv1.2</property>
					</properties>
				</ssl>
            </network>
			<properties>
				<property name="hazelcast.heartbeat.interval.seconds">%d</property>
			</properties>
        </hazelcast>
	`, clusterName, publicAddr, port, heartBeatSec)
}
