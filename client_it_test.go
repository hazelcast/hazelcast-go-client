package hazelcast_test

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestLifecycleEvents(t *testing.T) {
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		receivedStates := []hz.LifecycleState{}
		receivedStatesMu := &sync.RWMutex{}
		if _, err := client.ListenLifecycleStateChange(func(event hz.LifecycleStateChanged) {
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
		if err := client.Start(); err != nil {
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

func TestMemberEvents(t *testing.T) {
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		handlerCalled := int32(0)
		it.Must(client.ListenMembershipStateChange(func(event cluster.MembershipStateChanged) {
			if atomic.CompareAndSwapInt32(&handlerCalled, 0, 1) {
				wg.Done()
			}

		}))
		it.Must(client.Start())
		wg.Wait()
	})
}

func TestHeartbeat(t *testing.T) {
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
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		it.Must(client.Start())
		if err := client.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := client.Shutdown(); err == nil {
			t.Fatalf("shutting down second time should return an error")
		}
	})
}

func TestClient_Start(t *testing.T) {
	it.TesterWithConfigBuilder(t, nil, func(t *testing.T, client *hz.Client) {
		it.Must(client.Start())
		if err := client.Start(); err == nil {
			t.Fatalf("starting second time should return an error")
		}
		if err := client.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := client.Shutdown(); err == nil {
			t.Fatalf("shutting down second time should return an error")
		}
		if err := client.Start(); err == nil {
			t.Fatalf("starting after shutdown should return an error")
		}
	})
}

func getClient(t *testing.T) *hz.Client {
	client, err := hz.StartNewClient()
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func getClientWithConfigBuilder(t *testing.T, clientConfig *hz.ConfigBuilder) *hz.Client {
	client, err := hz.StartNewClientWithConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func getClientSmart(t *testing.T) *hz.Client {
	return getClient(t)
}

func getClientNonSmart(t *testing.T) *hz.Client {
	cb := hz.NewConfigBuilder()
	cb.Cluster().SetSmartRouting(false)
	return getClientWithConfigBuilder(t, cb)
}

func test(t *testing.T, f func(t *testing.T, client *hz.Client)) {
	var client *hz.Client
	t.Logf("testing smart client")
	client = getClientSmart(t)
	f(t, client)
	client.Shutdown()

	t.Logf("testing non-smart client")
	client = getClientNonSmart(t)
	f(t, client)
	client.Shutdown()
}
