package hztypes_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/logger"

	"github.com/hazelcast/hazelcast-go-client/internal/it"

	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/predicate"

	hz "github.com/hazelcast/hazelcast-go-client"
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
		it.MustValue(m.Put("key", targetValue))
		if ok := it.MustBool(m.ContainsKey("key")); !ok {
			t.Fatalf("key not found")
		}
		if ok := it.MustBool(m.ContainsValue("value")); !ok {
			t.Fatalf("value not found")
		}
		if value := it.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil!= %v", value)
		}
	})
}

func TestReplicatedMapRemove(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		targetValue := "value"
		it.MustValue(m.Put("key", targetValue))
		if !it.MustBool(m.ContainsKey("key")) {
			t.Fatalf("key not found")
		}
		if value, err := m.Remove("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target nil != %v", value)
		}
		if it.MustBool(m.ContainsKey("key")) {
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
		it.MustValue(m.Put("k1", "v1"))
		it.MustValue(m.Put("k2", "v2"))
		it.MustValue(m.Put("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
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
		it.MustValue(m.Put("k1", "v1"))
		it.MustValue(m.Put("k2", "v2"))
		it.MustValue(m.Put("k3", "v3"))
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
		if err := m.ListenEntryNotification(1, handler); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if !handlerCalled {
			t.Fatalf("handler was not called")
		}
		handlerCalled = false
		if err := m.UnlistenEntryNotification(1); err != nil {
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

func TestReplicatedMapEntryNotifiedEventWithKey(t *testing.T) {
	replicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m hztypes.ReplicatedMap) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		if err := m.ListenEntryNotificationToKey("k1", 1, handler); err != nil {
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
		it.MustValue(m.Put("k2", "v1"))
		time.Sleep(1 * time.Second)
		if handlerCalled {
			t.Fatalf("handler was called")
		}
	})
}

func TestReplicatedMapEntryNotifiedEventWithPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	replicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m hztypes.ReplicatedMap) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		if err := m.ListenEntryNotificationWithPredicate(predicate.Equal("A", "foo"), 1, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if !handlerCalled {
			t.Fatalf("handler was not called")
		}
		handlerCalled = false
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
		time.Sleep(1 * time.Second)
		if handlerCalled {
			t.Fatalf("handler was called")
		}
	})
}

func TestReplicatedMapEntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	replicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m hztypes.ReplicatedMap) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		if err := m.ListenEntryNotificationToKeyWithPredicate("k1", predicate.Equal("A", "foo"), 1, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if !handlerCalled {
			t.Fatalf("handler was not called")
		}
		handlerCalled = false
		it.MustValue(m.Put("k2", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if handlerCalled {
			t.Fatalf("handler was called")
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
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
		client, m = getClientReplicatedMapWithConfig("my-map", cb)
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
		client, m = getClientReplicatedMapWithConfig("my-map", cb)
		defer func() {
			if err := m.Clear(); err != nil {
				panic(err)
			}
			client.Shutdown()
		}()
		f(t, m)
	})
}

func getClientReplicatedMapWithConfig(name string, cb *hz.ConfigBuilder) (*hz.Client, hztypes.ReplicatedMap) {
	cb.Logger().SetLevel(logger.TraceLevel)
	client, err := hz.StartNewClientWithConfig(cb)
	if err != nil {
		panic(err)
	}
	mapName := fmt.Sprintf("%s-%d", name, rand.Int())
	fmt.Println("Map Name:", mapName)
	if m, err := client.GetReplicatedMap(mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}
