package hazelcast_test

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
)

func TestLifecycleEvents(t *testing.T) {
	receivedStates := []lifecycle.State{}
	receivedStatesMu := &sync.Mutex{}
	client := hz.MustClient(hz.NewClient())
	client.ListenLifecycleStateChange(func(event lifecycle.StateChanged) {
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

func getClient(t *testing.T) *hz.Client {
	client, err := hz.StartNewClient()
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func getClientWithConfig(t *testing.T, clientConfig hz.Config) *hz.Client {
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
	cb := hz.NewClientConfigBuilder()
	cb.Cluster().SetSmartRouting(false)
	if config, err := cb.Config(); err != nil {
		panic(err)
	} else {
		return getClientWithConfig(t, config)
	}
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
