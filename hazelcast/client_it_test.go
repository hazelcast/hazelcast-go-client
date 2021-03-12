package hazelcast_test

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
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
	m, err := client.GetMap("my-map")
	if err != nil {
		t.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			targetValue := "value"
			if _, err := m.Put("key", targetValue); err != nil {
				t.Fatal(err)
			}
			if value, err := m.Get("key"); err != nil {
				t.Fatal(err)
			} else if targetValue != value {
				t.Fatalf("target %v != %v", targetValue, value)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestLifecycleEvents(t *testing.T) {
	receivedStates := []lifecycle.State{}
	receivedStatesMu := &sync.Mutex{}
	client := hazelcast.NewClient()
	client.ListenLifecycleStateChange(func(event lifecycle.StateChanged) {
		receivedStatesMu.Lock()
		defer receivedStatesMu.Unlock()
		switch event.State() {
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
	targetStates := []lifecycle.State{lifecycle.StateClientConnected, lifecycle.StateClientDisconnected}
	if !reflect.DeepEqual(targetStates, receivedStates) {
		t.Fatalf("target %v != %v", targetStates, receivedStates)
	}
}
