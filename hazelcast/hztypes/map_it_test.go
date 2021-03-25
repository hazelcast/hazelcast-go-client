package hztypes_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
)

func TestPutGetMap(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetValue := "value"
		if _, err := m.Put("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestSetGetMap(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetValue := "value"
		if err := m.Set("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestSetGet1000(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		const setGetCount = 1000
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			hz.Must(m.Set(key, value))
		}
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			targetValue := fmt.Sprintf("v%d", i)
			assertEquals(t, targetValue, hz.MustValue(m.Get(key)))
		}
	})
}

func TestDelete(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetValue := "value"
		hz.Must(m.Set("key", targetValue))
		if value := hz.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Delete("key"); err != nil {
			t.Fatal(err)
		}
		if value := hz.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMapEvict(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetValue := "value"
		if err := m.Set("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Evict("key"); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target true != %v", value)
		}
	})
}

func TestMapClearSetGet(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetValue := "value"
		hz.Must(m.Set("key", targetValue))
		if ok := hz.MustBool(m.ContainsKey("key")); !ok {
			t.Fatalf("key not found")
		}
		if ok := hz.MustBool(m.ContainsValue("value")); !ok {
			t.Fatalf("value not found")
		}
		if value := hz.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(); err != nil {
			t.Fatal(err)
		}
		if value := hz.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil!= %v", value)
		}
	})
}

func TestRemove(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
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
	})
}

func TestGetAll(t *testing.T) {
	t.SkipNow()
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetMap := map[interface{}]interface{}{
			"k1": "v1",
			"k3": "v3",
		}
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
	})
}

func TestGetKeySet(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		hz.Must(m.Set("k1", "v1"))
		hz.Must(m.Set("k2", "v2"))
		hz.Must(m.Set("k3", "v3"))
		time.Sleep(1 * time.Second)
		assertEquals(t, "v1", hz.MustValue(m.Get("k1")))
		assertEquals(t, "v2", hz.MustValue(m.Get("k2")))
		assertEquals(t, "v3", hz.MustValue(m.Get("k3")))
		if keys, err := m.GetKeySet(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeStringSet(targetKeySet), makeStringSet(keys)) {
			t.Fatalf("target: %#v != %#v", targetKeySet, keys)
		}
	})
}

// TODO: Test Map Flush
// TODO: Test Map ForceUnlock
// TODO: Test Map GetKeySet
// TODO: Test Map GetValues
// TODO: Test Map IsEmpty
// TODO: Test Map IsLocked
// TODO: Test Map LoadAll
// TODO: Test Map LoadAllReplacingExisting
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

func TestMapEntryNotifiedEvent(t *testing.T) {
	testMap(t, func(t *testing.T, client hz.Client, m hztypes.Map) {
		handlerCalled := false
		flags := hztypes.NotifyEntryAdded | hztypes.NotifyEntryUpdated
		handler := func(event hztypes.EntryNotifiedEvent) {
			handlerCalled = true
		}
		if err := m.ListenEntryNotificationIncludingValue(flags, handler); err != nil {
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
		if err := m.UnlistenEntryNotification(handler); err != nil {
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
	})
}

func getClientMap(t *testing.T, name string) (hz.Client, hztypes.Map) {
	client, err := hz.StartNewClient()
	if err != nil {
		t.Fatal(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("MAPNAME:", mapName)
	if m, err := client.GetMap(mapName); err != nil {
		t.Fatal(err)
		return nil, nil
	} else {
		return client, m
	}
}

func getClientMapWithConfig(t *testing.T, name string, clientConfig hz.Config) (hz.Client, hztypes.Map) {
	client, err := hz.StartNewClientWithConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("MAPNAME:", mapName)
	if m, err := client.GetMap(mapName); err != nil {
		t.Fatal(err)
		return nil, nil
	} else {
		return client, m
	}
}

func getClientMapSmart(t *testing.T, name string) (hz.Client, hztypes.Map) {
	return getClientMap(t, name)
}

func getClientMapNonSmart(t *testing.T, name string) (hz.Client, hztypes.Map) {
	cb := hz.NewClientConfigBuilder()
	cb.Network().SetSmartRouting(false)
	if config, err := cb.Config(); err != nil {
		panic(err)
	} else {
		return getClientMapWithConfig(t, name, config)
	}
}

func assertEquals(t *testing.T, target, value interface{}) {
	if !reflect.DeepEqual(target, value) {
		t.Fatalf("target: %#v != %#v", target, value)
	}
}

func makeStringSet(items []interface{}) map[string]struct{} {
	result := map[string]struct{}{}
	for _, item := range items {
		result[item.(string)] = struct{}{}
	}
	return result
}

func testMap(t *testing.T, f func(t *testing.T, client hz.Client, m hztypes.Map)) {
	var (
		client hz.Client
		m      hztypes.Map
	)
	t.Logf("testing smart client")
	client, m = getClientMapSmart(t, "my-map")
	f(t, client, m)
	client.Shutdown()

	t.Logf("testing non-smart client")
	client, m = getClientMapNonSmart(t, "my-map")
	f(t, client, m)
	client.Shutdown()
}
