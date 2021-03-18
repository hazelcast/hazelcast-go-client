package hazelcast_test

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewClientGetMap(t *testing.T) {
	client := hazelcast.NewClient()
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	m, err := client.GetMap("1my-map")
	if err != nil {
		t.Fatal(err)
	}
	targetValue := "value"
	if _, err := m.Put("key", targetValue); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
	} else if targetValue != value {
		t.Fatalf("target %v != %v", targetValue, value)
	}
	client.Shutdown()
}

func TestLifecycleEvents(t *testing.T) {
	receivedStates := []lifecycle.State{}
	receivedStatesMu := &sync.Mutex{}
	client := hazelcast.NewClient()
	client.ListenLifecycleStateChange(func(event lifecycle.StateChanged) {
		receivedStatesMu.Lock()
		defer receivedStatesMu.Unlock()
		switch event.State() {
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
			fmt.Println("Received unknown state:", event.State())
		}
		receivedStates = append(receivedStates, event.State())
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

func TestMapEntryAddedEvent(t *testing.T) {
	client := hazelcast.NewClient()
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	m, err := client.GetMap("test-map4")
	if err != nil {
		t.Fatal(err)
	}
	handlerCalled := false
	flags := hztypes.NotifyEntryAdded | hztypes.NotifyEntryUpdated
	if err := m.ListenEntryNotified(flags, true, func(event hztypes.EntryNotifiedEvent) {
		handlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put("k1", "v1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if !handlerCalled {
		t.Fatalf("handler was not called")
	}
}
