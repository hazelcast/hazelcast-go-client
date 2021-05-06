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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestClientLifecycleEvents(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		receivedStates := []hz.LifecycleState{}
		receivedStatesMu := &sync.RWMutex{}
		if _, err := client.AddLifecycleListener(func(event hz.LifecycleStateChanged) {
			receivedStatesMu.Lock()
			defer receivedStatesMu.Unlock()
			switch event.State {
			case hz.LifecycleStateStarting:
				fmt.Println("Received starting state")
			case hz.LifecycleStateStarted:
				fmt.Println("Received started state")
			case hz.LifecycleStateShuttingDown:
				fmt.Println("Received shutting down state")
			case hz.LifecycleStateShutDown:
				fmt.Println("Received shut down state")
			case hz.LifecycleStateClientConnected:
				fmt.Println("Received client connected state")
			case hz.LifecycleStateClientDisconnected:
				fmt.Println("Received client disconnected state")
			default:
				fmt.Println("Received unknown state:", event.State)
			}
			receivedStates = append(receivedStates, event.State)
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Millisecond)
		if err := client.Shutdown(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Millisecond)
		targetStates := []hz.LifecycleState{
			hz.LifecycleStateStarting,
			hz.LifecycleStateClientConnected,
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

/*
func TestClientMemberEvents(t *testing.T) {
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		handlerCalled := int32(0)
		it.Must(client.AddMembershipListener(func(event cluster.MembershipStateChanged) {
			if atomic.CompareAndSwapInt32(&handlerCalled, 0, 1) {
				wg.Done()
			}

		}))
		it.Must(client.Start())
		wg.Wait()
	})
}
*/

func TestClientHeartbeat(t *testing.T) {
	// Slow test.
	t.SkipNow()
	it.MapTesterWithConfigBuilder(t, func(cb *hz.ConfigBuilder) {
	}, func(t *testing.T, m *hz.Map) {
		time.Sleep(150 * time.Second)
		target := "v1"
		it.Must(m.Set("k1", target))
		if v := it.MustValue(m.Get("k1")); target != v {
			t.Fatalf("target: %v != %v", target, v)
		}
	})
}

func TestClient_Shutdown(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		if err := client.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := client.Shutdown(); err == nil {
			t.Fatalf("shutting down second time should return an error")
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
				client.Shutdown()
			}()
		}
		wg.Wait()
	})
}
