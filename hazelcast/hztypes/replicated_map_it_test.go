package hztypes_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/it"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/property"
)

func TestPutGetReplicatedMap(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
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

func TestReplicatedMapClearSetGet(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		targetValue := "value"
		hz.MustValue(m.Put("key", targetValue))
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

func TestReplicatedMapRemove(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		targetValue := "value"
		hz.MustValue(m.Put("key", targetValue))
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

func TestReplicatedMapGetEntrySet(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		target := []hztypes.Entry{
			hztypes.NewEntry("k1", "v1"),
			hztypes.NewEntry("k2", "v2"),
			hztypes.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(target); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if entries, err := m.GetEntrySet(); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestReplicatedMapGetKeySet(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		hz.MustValue(m.Put("k1", "v1"))
		hz.MustValue(m.Put("k2", "v2"))
		hz.MustValue(m.Put("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", hz.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", hz.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", hz.MustValue(m.Get("k3")))
		if keys, err := m.GetKeySet(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeStringSet(targetKeySet), makeStringSet(keys)) {
			t.Fatalf("target: %#v != %#v", targetKeySet, keys)
		}
	})
}

func TestReplicatedMapIsEmptySize(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target: true != false")
		}
		targetSize := 0
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		hz.MustValue(m.Put("k1", "v1"))
		hz.MustValue(m.Put("k2", "v2"))
		hz.MustValue(m.Put("k3", "v3"))
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if value {
			t.Fatalf("target: false != true")
		}
		targetSize = 3
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
	})
}

func TestReplicatedMapEntryNotifiedEvent(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		// TODO: remove the following sleep once we dynamically add connection listeners
		time.Sleep(2 * time.Second)
		listenerConfig := hztypes.MapEntryListenerConfig{}
		if err := m.ListenEntryNotification(listenerConfig, handler); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if !handlerCalled {
			t.Fatalf("handler was not called")
		}
		handlerCalled = false
		if err := m.UnlistenEntryNotification(handler); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if handlerCalled {
			t.Fatalf("handler was called")
		}
	})
}

func replicatedMapTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m hztypes.ReplicatedMap)) {
	var (
		client *hz.Client
		m      hztypes.ReplicatedMap
	)
	t.Run("Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		client, m = getClientReplicatedMapWithConfig("my-map", config)
		defer func() {
			if err := m.Clear(); err != nil {
				panic(err)
			}
			client.Shutdown()
		}()
		f(t, m)

	})
	t.Run("Non-Smart Client", func(t *testing.T) {
		cb := hz.NewConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Cluster().SetSmartRouting(false)
		config, err := cb.Config()
		if err != nil {
			panic(err)
		}
		config.ClusterConfig.SmartRouting = false
		client, m = getClientReplicatedMapWithConfig("my-map", config)
		defer func() {
			if err := m.Clear(); err != nil {
				panic(err)
			}
			client.Shutdown()
		}()
		f(t, m)
	})
}

func getClientReplicatedMapWithConfig(name string, clientConfig hz.Config) (*hz.Client, hztypes.ReplicatedMap) {
	clientConfig.Properties[property.LoggingLevel] = "trace"
	client, err := hz.StartNewClientWithConfig(clientConfig)
	if err != nil {
		panic(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("Map Name:", mapName)
	if m, err := client.GetReplicatedMap(mapName); err != nil {
		panic(err)
		return nil, nil
	} else {
		return client, m
	}
}
