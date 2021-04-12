package hazelcast_test

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/logger"

	"github.com/hazelcast/hazelcast-go-client/cluster"

	"github.com/hazelcast/hazelcast-go-client/hztypes"

	"github.com/hazelcast/hazelcast-go-client/internal/it"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/lifecycle"
)

func TestLifecycleEvents(t *testing.T) {
	receivedStates := []lifecycle.State{}
	receivedStatesMu := &sync.Mutex{}
	client := it.MustClient(hz.NewClient())
	client.ListenLifecycleStateChange(1, func(event lifecycle.StateChanged) {
		receivedStatesMu.Lock()
		defer receivedStatesMu.Unlock()
		switch event.State {
		case lifecycle.StateStarting:
			fmt.Println("Received starting state")
		case lifecycle.StateStarted:
			fmt.Println("Received started state")
		case lifecycle.StateShuttingDown:
			fmt.Println("Received shutting down state")
		case lifecycle.StateShutDown:
			fmt.Println("Received shut down state")
		case lifecycle.StateClientConnected:
			fmt.Println("Received client connected state")
		case lifecycle.StateClientDisconnected:
			fmt.Println("Received client disconnected state")
		default:
			fmt.Println("Received unknown state:", event.State)
		}
		receivedStates = append(receivedStates, event.State)
	})
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Millisecond)
	client.Shutdown()
	time.Sleep(1 * time.Millisecond)
	targetStates := []lifecycle.State{
		lifecycle.StateStarting,
		lifecycle.StateClientConnected,
		lifecycle.StateStarted,
		lifecycle.StateShuttingDown,
		lifecycle.StateShutDown,
	}
	if !reflect.DeepEqual(targetStates, receivedStates) {
		t.Fatalf("target %v != %v", targetStates, receivedStates)
	}
}

func TestMemberEvents(t *testing.T) {
	cb := hz.NewConfigBuilder()
	cb.Logger().SetLevel(logger.TraceLevel)
	client := it.MustClient(hz.NewClient())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	handlerCalled := false
	client.ListenMemberStateChange(1, func(event cluster.MemberStateChanged) {
		if !handlerCalled {
			handlerCalled = true
			wg.Done()
		}

	})
	it.Must(client.Start())
	wg.Wait()
}

func TestHeartbeat(t *testing.T) {
	// Slow test.
	t.SkipNow()
	it.MapTesterWithConfigBuilder(t, func(cb *hz.ConfigBuilder) {
	}, func(t *testing.T, m hztypes.Map) {
		time.Sleep(150 * time.Second)
		target := "v1"
		it.Must(m.Set("k1", target))
		if v := it.MustValue(m.Get("k1")); target != v {
			t.Fatalf("target: %v != %v", target, v)
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
