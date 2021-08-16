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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/types"
)

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
	cls := it.StartNewClusterWithOptions("go-cli-test-cluster", 15701, 3)
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

	cls = it.StartNewClusterWithOptions("go-cli-test-cluster", 15701, 3)
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

func TestClient_GetDistributedObjects(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		var (
			testMapName = it.NewUniqueObjectName("map")
			testSetName = it.NewUniqueObjectName("set")
			mapInfo     = types.NewDistributedObjectInfo(testMapName, hz.ServiceNameMap)
			setInfo     = types.NewDistributedObjectInfo(testSetName, hz.ServiceNameSet)
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
