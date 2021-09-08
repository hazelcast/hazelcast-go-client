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
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var idGen = proxy.ReferenceIDGenerator{}

func TestClientLifecycleEvents(t *testing.T) {
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
		time.Sleep(1 * time.Millisecond)
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Millisecond)
		targetStates := []hz.LifecycleState{
			hz.LifecycleStateStarting,
			hz.LifecycleStateConnected,
			hz.LifecycleStateStarted,
			hz.LifecycleStateShuttingDown,
			hz.LifecycleStateShutDown,
		}
		receivedStatesMu.RLock()
		defer receivedStatesMu.RUnlock()
		if !reflect.DeepEqual(targetStates, receivedStates) {
			t.Fatalf("target %v != %v", targetStates, receivedStates)
		}
	})
}

func TestClientRunning(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		assert.True(t, client.Running())
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		assert.False(t, client.Running())
	})
}

func TestClientPortRangeAllAddresses(t *testing.T) {
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{"127.0.0.1", "localhost", "0.0.0.0"}
	}, 4, 3)
}

func TestClientPortRangeMultipleAddresses(t *testing.T) {
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{"127.0.0.1", "localhost", fmt.Sprintf("0.0.0.0:%d", validPort)}
	}, 4, 3)
}

func TestClientPortRangeSingleAddress(t *testing.T) {
	portRangeConnectivityTest(t, func(validPort int) []string {
		return []string{fmt.Sprintf("localhost:%d", validPort), "127.0.0.1", fmt.Sprintf("0.0.0.0:%d", validPort)}
	}, 4, 3)
}

func TestClientMemberEvents(t *testing.T) {
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

func TestClientHeartbeat(t *testing.T) {
	// Slow test.
	t.SkipNow()
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

func TestClient_Shutdown(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutting down second time should not return an error")
		}
	})
}

func TestClientShutdownRace(t *testing.T) {
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

func TestClient_AddDistributedObjectListener(t *testing.T) {
	type objInfo struct {
		service string
		object  string
		count   int
	}
	createDestroyMap := func(client *hz.Client, mapName string) {
		m := it.MustValue(client.GetMap(context.Background(), mapName)).(*hz.Map)
		time.Sleep(100 * time.Millisecond)
		it.Must(m.Destroy(context.Background()))
		time.Sleep(100 * time.Millisecond)
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
		time.Sleep(1 * time.Second)
		createDestroyMap(client, "dolistener-tester")
		targetObjInfo := objInfo{service: hz.ServiceNameMap, object: "dolistener-tester", count: 1}
		mu.Lock()
		if !assert.Equal(t, targetObjInfo, created) {
			t.FailNow()
		}
		if !assert.Equal(t, targetObjInfo, destroyed) {
			t.FailNow()
		}
		mu.Unlock()

		if err := client.RemoveDistributedObjectListener(context.Background(), subID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		createDestroyMap(client, "dolistener-tester")
		mu.Lock()
		if !assert.Equal(t, targetObjInfo, created) {
			t.FailNow()
		}
		if !assert.Equal(t, targetObjInfo, destroyed) {
			t.FailNow()
		}
		mu.Unlock()
	})
}

func TestClusterReconnection_ShutdownCluster(t *testing.T) {
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions("go-cli-test-cluster", 15701, it.MemberCount())
	mu := &sync.Mutex{}
	events := []hz.LifecycleState{}
	config := cls.DefaultConfig()
	config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
		mu.Lock()
		events = append(events, event.State)
		mu.Unlock()
	})
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	cls.Shutdown()
	time.Sleep(5 * time.Second)
	cls = it.StartNewClusterWithOptions("go-cli-test-cluster", 15701, it.MemberCount())
	time.Sleep(5 * time.Second)
	cls.Shutdown()
	c.Shutdown(ctx)
	mu.Lock()
	defer mu.Unlock()
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
	t.Logf("target : %v", target)
	t.Logf("events : %v", events)
	assert.Equal(t, target, events)
}

func TestClusterReconnection_RemoveMembersOneByOne(t *testing.T) {
	ctx := context.Background()
	clusterName := fmt.Sprintf("go-cli-test-cluster-%d", idGen.NextID())
	cls := it.StartNewClusterWithOptions(clusterName, 11701, 3)
	mu := &sync.Mutex{}
	var events []hz.LifecycleState
	config := cls.DefaultConfig()
	config.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
		mu.Lock()
		events = append(events, event.State)
		mu.Unlock()
	})
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)

	// start shutting down members one by one
	for _, uuid := range cls.MemberUUIDs {
		time.Sleep(1 * time.Second)
		cls.RC.ShutdownMember(ctx, cls.ClusterID, uuid)
	}
	time.Sleep(1 * time.Second)
	cls.Shutdown()
	time.Sleep(1 * time.Second)

	cls = it.StartNewClusterWithOptions(clusterName, 11701, 3)
	time.Sleep(5 * time.Second)
	// start shutting down members one by one
	for _, uuid := range cls.MemberUUIDs {
		time.Sleep(1 * time.Second)
		cls.RC.ShutdownMember(ctx, cls.ClusterID, uuid)
	}
	time.Sleep(1 * time.Second)
	c.Shutdown(ctx)

	mu.Lock()
	defer mu.Unlock()
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
	t.Logf("target : %v", target)
	t.Logf("events : %v", events)
	assert.Equal(t, target, events)
}

func TestClusterReconnection_ReconnectModeOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cls := it.StartNewClusterWithOptions("go-cli-test-cluster", 15701, it.MemberCount())
	config := cls.DefaultConfig()
	config.Cluster.ConnectionStrategy.ReconnectMode = cluster.ReconnectModeOff
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	cls.Shutdown()
	time.Sleep(100 * time.Millisecond)
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

func TestClient_GetDistributedObjects(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var (
			testMapName = it.NewUniqueObjectName("map")
			testSetName = it.NewUniqueObjectName("set")
			mapInfo     = types.DistributedObjectInfo{Name: testMapName, ServiceName: hz.ServiceNameMap}
			setInfo     = types.DistributedObjectInfo{Name: testSetName, ServiceName: hz.ServiceNameSet}
		)

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()

		testMap, err := client.GetMap(ctx, testMapName)
		if err != nil {
			t.Fatal(err)
		}
		testSet, err := client.GetSet(ctx, testSetName)
		if err != nil {
			t.Fatal(err)
		}
		objects, err := client.GetDistributedObjectsInfo(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Contains(t, objects, mapInfo)
		assert.Contains(t, objects, setInfo)

		if err = testMap.Destroy(ctx); err != nil {
			t.Fatal(err)
		}
		if err = testSet.Destroy(ctx); err != nil {
			t.Fatal(err)
		}

		objects, err = client.GetDistributedObjectsInfo(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assert.NotContains(t, objects, mapInfo)
		assert.NotContains(t, objects, setInfo)
	})
}

func TestClient_GetProxyInstance(t *testing.T) {
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

func TestClientFailover_OSSCluster(t *testing.T) {
	it.SkipIf(t, "enterprise")
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions("failover-test-cluster", 15701, it.MemberCount())
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

func TestClientFailover_EECluster(t *testing.T) {
	it.SkipIf(t, "oss")
	ctx := context.Background()
	cls := it.StartNewClusterWithOptions("failover-test-cluster", 15701, it.MemberCount())
	defer cls.Shutdown()
	config := cls.DefaultConfig()
	config.Failover.Enabled = true
	config.Failover.TryCount = 1
	// move the main cluster config to failover config list
	config.Failover.SetConfigs(config.Cluster)
	// use a non-existing cluster in the main cluster config
	config.Cluster.Name = "non-existing-failover-test-cluster"
	c, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("should have connected to failover cluster")
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClientFailover_EECluster_Reconnection(t *testing.T) {
	it.SkipIf(t, "oss")
	ctx := context.Background()
	cls1 := it.StartNewClusterWithOptions("failover-test-cluster1", 15701, it.MemberCount())
	cls2 := it.StartNewClusterWithOptions("failover-test-cluster2", 16701, it.MemberCount())
	defer cls2.Shutdown()
	var wg sync.WaitGroup
	wg.Add(1)
	config := cls1.DefaultConfig()
	config.Logger.Level = logger.DebugLevel
	config.Failover.Enabled = true
	config.Failover.TryCount = 1
	failoverConfig := config.Cluster
	failoverConfig.Name = "failover-test-cluster2"
	failoverConfig.Network.SetAddresses(fmt.Sprintf("localhost:%d", 15702))
	config.Failover.SetConfigs(failoverConfig)
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

func highlight(t *testing.T, format string, args ...interface{}) {
	log.Printf("\n===\n%s\n===", fmt.Sprintf(format, args...))
}

func TestClientFixConnection(t *testing.T) {
	// This test removes the member that corresponds to the connections which receives membership state changes.
	// Once that connection is closed, another connection should be randomly selected to receive membership state changes.
	// A new member is added to confirm that is the case.
	const memberCount = 3
	addedCount := int64(0)
	ctx := context.Background()
	id := idGen.NextID()
	clusterName := fmt.Sprintf("600-cluster-%d", id)
	log.Println("Cluster name:", clusterName)
	port := 20701 + id*10
	cls := it.StartNewClusterWithOptions(clusterName, int(port), memberCount)
	defer cls.Shutdown()
	config := hz.Config{}
	config.Cluster.Network.SetAddresses(fmt.Sprintf("localhost:%d", port+1))
	config.Cluster.Name = clusterName
	config.AddMembershipListener(func(event cluster.MembershipStateChanged) {
		highlight(t, "%s member: %s", event.State.String(), event.Member.UUID)
		if event.State == cluster.MembershipStateAdded {
			atomic.AddInt64(&addedCount, 1)
		}
	})
	if it.TraceLoggingEnabled() {
		config.Logger.Level = logger.TraceLevel
	}
	client, err := hz.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Shutdown(ctx)
	// terminate the member that corresponds to the connection which receives cluster membership updates
	mUUID := cls.MemberUUIDs[1]
	highlight(t, "Terminated member: %s", mUUID)
	ok, err := cls.RC.TerminateMember(ctx, cls.ClusterID, mUUID)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		log.Fatalf("could not terminate member: %s", err.Error())
	}
	m, err := cls.RC.StartMember(ctx, cls.ClusterID)
	if err != nil {
		log.Fatal(err)
	}
	highlight(t, "Started member: %s", m.UUID)
	time.Sleep(30 * time.Second)
	assert.Equal(t, int64(memberCount+1), atomic.LoadInt64(&addedCount))
}

func TestClientVersion(t *testing.T) {
	// adding this test here, so there's no "unused lint warning.
	assert.Equal(t, "1.1.0", hz.ClientVersion)
}
