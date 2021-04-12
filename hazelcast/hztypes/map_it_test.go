package hztypes_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/predicate"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/it"
)

func TestPutGetMap(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
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
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
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
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		const setGetCount = 1000
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			it.Must(m.Set(key, value))
		}
		for i := 0; i < setGetCount; i++ {
			key := fmt.Sprintf("k%d", i)
			targetValue := fmt.Sprintf("v%d", i)
			it.AssertEquals(t, targetValue, it.MustValue(m.Get(key)))
		}
	})
}

func TestDelete(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
		if value := it.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Delete("key"); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil != %v", value)
		}
	})
}

func TestMapEvict(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
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
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
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

func TestMapRemove(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		targetValue := "value"
		it.Must(m.Set("key", targetValue))
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

func TestGetAll(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		const maxKeys = 3
		makeKey := func(id int) string {
			return fmt.Sprintf("k%d", id)
		}
		makeValue := func(id int) string {
			return fmt.Sprintf("v%d", id)
		}
		var allPairs []hztypes.Entry
		for i := 0; i < maxKeys; i++ {
			allPairs = append(allPairs, hztypes.NewEntry(makeKey(i), makeValue(i)))
		}
		var keys []interface{}
		var target []hztypes.Entry
		for i, pair := range allPairs {
			if i%2 == 0 {
				keys = append(keys, pair.Key)
				target = append(target, pair)
			}
		}
		for _, pair := range allPairs {
			it.Must(m.Set(pair.Key, pair.Value))
		}
		time.Sleep(1 * time.Second)
		for _, pair := range allPairs {
			it.AssertEquals(t, pair.Value, it.MustValue(m.Get(pair.Key)))
		}
		if kvs, err := m.GetAll(keys...); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, kvs) {
			t.Fatalf("target: %#v != %#v", target, kvs)
		}
	})
}

func TestMapGetKeySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		it.Must(m.Set("k1", "v1"))
		it.Must(m.Set("k2", "v2"))
		it.Must(m.Set("k3", "v3"))
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
func TestMapGetValues(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		targetValues := []interface{}{"v1", "v2", "v3"}
		it.Must(m.Set("k1", "v1"))
		it.Must(m.Set("k2", "v2"))
		it.Must(m.Set("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
		if values, err := m.GetValues(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeInterfaceSet(targetValues), makeInterfaceSet(values)) {
			t.Fatalf("target: %#v != %#v", targetValues, values)
		}
	})
}

func TestPutAll(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		pairs := []hztypes.Entry{
			hztypes.NewEntry("k1", "v1"),
			hztypes.NewEntry("k2", "v2"),
			hztypes.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(pairs); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
	})
}

func TestMapGetEntrySet(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
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

func TestGetEntrySetWithPredicateUsingPortable(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m hztypes.Map) {
		entries := []hztypes.Entry{
			hztypes.NewEntry("k1", &it.SamplePortable{A: "foo", B: 10}),
			hztypes.NewEntry("k2", &it.SamplePortable{A: "foo", B: 15}),
			hztypes.NewEntry("k3", &it.SamplePortable{A: "foo", B: 10}),
		}
		if err := m.PutAll(entries); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []hztypes.Entry{
			hztypes.NewEntry("k1", &it.SamplePortable{A: "foo", B: 10}),
			hztypes.NewEntry("k3", &it.SamplePortable{A: "foo", B: 10}),
		}
		if entries, err := m.GetEntrySetWithPredicate(predicate.And(predicate.Equal("A", "foo"), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestGetEntrySetWithPredicateUsingJSON(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		entries := []hztypes.Entry{
			hztypes.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.JSONValue()),
			hztypes.NewEntry("k2", it.SamplePortable{A: "foo", B: 15}.JSONValue()),
			hztypes.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.JSONValue()),
		}
		if err := m.PutAll(entries); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		target := []hztypes.Entry{
			hztypes.NewEntry("k1", it.SamplePortable{A: "foo", B: 10}.JSONValue()),
			hztypes.NewEntry("k3", it.SamplePortable{A: "foo", B: 10}.JSONValue()),
		}
		if entries, err := m.GetEntrySetWithPredicate(predicate.And(predicate.Equal("A", "foo"), predicate.Equal("B", 10))); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestGetEntryView(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		it.Must(m.Set("k1", "v1"))
		if ev, err := m.GetEntryView("k1"); err != nil {
			t.Fatal(err)
		} else {
			it.AssertEquals(t, "k1", ev.Key())
			it.AssertEquals(t, "v1", ev.Value())
		}
	})
}

// TODO: Test Map AddIndex
// TODO: Test Map AddInterceptor
// TODO: Test Map ExecuteOnEntries
// TODO: Test Map Flush
// TODO: Test Map ForceUnlock
// TODO: Test Map GetEntrySet
// TODO: Test Map IsLocked
// TODO: Test Map LoadAll
// TODO: Test Map LoadAllReplacingExisting
// TODO: Test Map Lock
// TODO: Test Map SetWithTTL
// TODO: Test Map TryLock
// TODO: Test Map TryLockWithLease
// TODO: Test Map TryLockWithTimeout
// TODO: Test Map TryLockWithLeaseTimeout
// TODO: Test Map TryPut
// TODO: Test Map TryPutWithTimeout
// TODO: Test Map TryRemove
// TODO: Test Map TryRemoveWithTimeout
// TODO: Test Map PutWithTTL
// TODO: Test Map PutWithMaxIdle
// TODO: Test Map PutWithTTLAndMaxIdle
// TODO: Test Map PutIfAbsent
// TODO: Test Map PutIfAbsentWithTTL
// TODO: Test Map PutTransient
// TODO: Test Map PutTransientWithTTL
// TODO: Test Map PutTransientWithMaxIdle
// TODO: Test Map PutTransientWithTTLMaxIdle
// TODO: Test Map RemoveIfSame
// TODO: Test Map Replace
// TODO: Test Map ReplaceIfSame

func TestMapIsEmptySize(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
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

func TestMapEntryNotifiedEvent(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		listenerConfig := hztypes.MapEntryListenerConfig{
			NotifyEntryAdded:   true,
			NotifyEntryUpdated: true,
			IncludeValue:       true,
		}
		if err := m.ListenEntryNotification(listenerConfig, 1, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", "v1"))
		time.Sleep(1 * time.Second)
		if !handlerCalled {
			t.Fatalf("handler was not called")
		}
		handlerCalled = false
		if err := m.UnlistenEntryNotification(1); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", "v1"))
		time.Sleep(1 * time.Second)
		if handlerCalled {
			t.Fatalf("handler was called")
		}
	})
}

func TestMapEntryNotifiedEventToKey(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m hztypes.Map) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		listenerConfig := hztypes.MapEntryListenerConfig{
			NotifyEntryAdded:   true,
			NotifyEntryUpdated: true,
			IncludeValue:       true,
			Key:                "k1",
		}
		if err := m.ListenEntryNotification(listenerConfig, 1, handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", "v1"))
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

func TestMapEntryNotifiedEventWithPredicate(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m hztypes.Map) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		listenerConfig := hztypes.MapEntryListenerConfig{
			NotifyEntryAdded:   true,
			NotifyEntryUpdated: true,
			IncludeValue:       true,
			Predicate:          predicate.Equal("A", "foo"),
		}
		if err := m.ListenEntryNotification(listenerConfig, 1, handler); err != nil {
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

func TestMapEntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m hztypes.Map) {
		handlerCalled := false
		handler := func(event *hztypes.EntryNotified) {
			handlerCalled = true
		}
		listenerConfig := hztypes.MapEntryListenerConfig{
			NotifyEntryAdded:   true,
			NotifyEntryUpdated: true,
			IncludeValue:       true,
			Key:                "k1",
			Predicate:          predicate.Equal("A", "foo"),
		}
		if err := m.ListenEntryNotification(listenerConfig, 1, handler); err != nil {
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

func makeStringSet(items []interface{}) map[string]struct{} {
	result := map[string]struct{}{}
	for _, item := range items {
		result[item.(string)] = struct{}{}
	}
	return result
}

func makeInterfaceSet(items []interface{}) map[interface{}]struct{} {
	result := map[interface{}]struct{}{}
	for _, item := range items {
		result[item] = struct{}{}
	}
	return result
}

func entriesEqualUnordered(p1, p2 []hztypes.Entry) bool {
	if len(p1) != len(p2) {
		return false
	}
	// inefficient
	for _, item1 := range p1 {
		if entriesIndex(item1, p2) < 0 {
			return false
		}
	}
	for _, item2 := range p2 {
		if entriesIndex(item2, p1) < 0 {
			return false
		}
	}
	return true
}

func entriesIndex(p hztypes.Entry, ps []hztypes.Entry) int {
	for i, item := range ps {
		if reflect.DeepEqual(p, item) {
			return i
		}
	}
	return -1
}
