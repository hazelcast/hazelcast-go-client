package hazelcast_test

import (
	"fmt"
	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPutGetMap(t *testing.T) {
	client, m := getClientMap(t, "my-map")
	targetValue := "value"
	if _, err := m.Put("key", targetValue); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
	} else if targetValue != value {
		t.Fatalf("target %v != %v", targetValue, value)
	}
	if value, err := m.Put("key", targetValue); err != nil {
		t.Fatal(err)
	} else if targetValue != value {
		t.Fatalf("target %v != %v", targetValue, value)
	}
	if err := m.Delete("key"); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
	} else if nil != value {
		t.Fatalf("target nil != %v", value)
	}
	client.Shutdown()
}

func TestSetGet100(t *testing.T) {
	const setGetCount = 1000
	client, m := getClientMap(t, "my-map")
	for i := 0; i < setGetCount; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		hz.Must(m.Set(key, value))
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < setGetCount; i++ {
		key := fmt.Sprintf("k%d", i)
		targetValue := fmt.Sprintf("v%d", i)
		assertEquals(t, targetValue, hz.MustValue(m.Get(key)))
	}
	client.Shutdown()
}

func TestMapEvict(t *testing.T) {
	client, m := getClientMap(t, "my-map")
	targetValue := "value"
	if err := m.Set("key", targetValue); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Evict("key"); err != nil {
		t.Fatal(err)
	} else if !value {
		t.Fatalf("target true != %v", value)
	}
	client.Shutdown()
}

func TestMapClearSetGet(t *testing.T) {
	client, m := getClientMap(t, "my-map")
	targetValue := "value"
	if err := m.Set("key", targetValue); err != nil {
		t.Fatal(err)
	}
	if value, err := m.ContainsKey("key"); err != nil {
		t.Fatal(err)
	} else if !value {
		t.Fatalf("key not found")
	}
	if value, err := m.ContainsValue("value"); err != nil {
		t.Fatal(err)
	} else if !value {
		t.Fatalf("value not found")
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
	} else if targetValue != value {
		t.Fatalf("target %v != %v", targetValue, value)
	}
	if err := m.Clear(); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Get("key"); err != nil {
		t.Fatal(err)
	} else if nil != value {
		t.Fatalf("target nil!= %v", value)
	}
	client.Shutdown()
}

func TestRemove(t *testing.T) {
	client, m := getClientMap(t, "my-map")
	targetValue := "value"
	hz.Must(m.Set("key", targetValue))
	if !hz.MustBool(m.ContainsKey("key")) {
		t.Fatalf("key not found")
	}
	if value, err := m.Remove("key"); err != nil {
		t.Fatal(err)
	} else if targetValue != value {
		t.Fatalf("target nil != %v", value)
	}
	if hz.MustBool(m.ContainsKey("key")) {
		t.Fatalf("key found")
	}
	client.Shutdown()
}

func TestGetAll(t *testing.T) {
	t.SkipNow()
	targetMap := map[interface{}]interface{}{
		"k1": "v1",
		"k3": "v3",
	}
	client, m := getClientMap(t, "my-map-getall")
	hz.Must(m.Set("k1", "v1"))
	hz.Must(m.Set("k2", "v2"))
	hz.Must(m.Set("k3", "v3"))
	time.Sleep(1 * time.Second)
	assertEquals(t, "v1", hz.MustValue(m.Get("k1")))
	assertEquals(t, "v2", hz.MustValue(m.Get("k2")))
	assertEquals(t, "v3", hz.MustValue(m.Get("k3")))
	if kvs, err := m.GetAll("k1", "k3"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(targetMap, kvs) {
		t.Fatalf("target: %#v != %#v", targetMap, kvs)
	}
	client.Shutdown()
}

// TODO: Test Map Flush
// TODO: Test Map ForceUnlock
// TODO: Test Map IsEmpty
// TODO: Test Map IsLocked
// TODO: Test Map Lock
// TODO: Test Map SetWithTTL
// TODO: Test Map Size
// TODO: Test Map TryLock
// TODO: Test Map TryLockWithLease
// TODO: Test Map TryLockWithTimeout
// TODO: Test Map TryLockWithLeaseTimeout
// TODO: Test Map TryPut
// TODO: Test Map TryPutWithTimeout
// TODO: Test Map TryRemove
// TODO: Test Map TryRemoveWithTimeout
// TODO: Test Map PutIfAbsent
// TODO: Test Map PutIfAbsentWithTTL
// TODO: Test Map PutTransient
// TODO: Test Map PutTransientWithTTL
// TODO: Test Map PutTransientWithMaxIdle
// TODO: Test Map PutTransientWithTTLMaxIdle
// TODO: Test Map RemoveIfSame
// TODO: Test Map Replace
// TODO: Test Map ReplaceIfSame

func TestLifecycleEvents(t *testing.T) {
	receivedStates := []lifecycle.State{}
	receivedStatesMu := &sync.Mutex{}
	client := hz.NewClient()
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

func TestMapEntryNotifiedEvent(t *testing.T) {
	client, m := getClientMap(t, "test-map4")
	handlerCalled := false
	flags := hztypes.NotifyEntryAdded | hztypes.NotifyEntryUpdated
	handler := func(event hztypes.EntryNotifiedEvent) {
		handlerCalled = true
	}
	if err := m.ListenEntryNotifiedIncludingValue(flags, handler); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if _, err := m.Put("k1", "v1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if !handlerCalled {
		t.Fatalf("handler was not called")
	}
	handlerCalled = false
	if err := m.UnlistenEntryNotified(handler); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if _, err := m.Put("k1", "v1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if handlerCalled {
		t.Fatalf("handler was called")
	}
	client.Shutdown()
}

func getClientMap(t *testing.T, name string) (hz.Client, hztypes.Map) {
	client, err := hz.StartNewClient()
	if err != nil {
		t.Fatal(err)
	}
	if m, err := client.GetMap(name); err != nil {
		t.Fatal(err)
		return nil, nil
	} else {
		return client, m
	}
}

func assertEquals(t *testing.T, target, value interface{}) {
	if !reflect.DeepEqual(target, value) {
		t.Fatalf("target: %#v != %#v", target, value)
	}
}
