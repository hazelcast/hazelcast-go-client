package proxy

import (
	"github.com/hazelcast/go-client"
	. "github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/rc"
	. "github.com/hazelcast/go-client/serialization"
	. "github.com/hazelcast/go-client/tests"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

var mp IMap
var client hazelcast.IHazelcastInstance

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp, _ = client.GetMap(&mapName)
	m.Run()
	mp.Clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestMapProxy_SinglePutGet(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
}
func TestMapProxy_ManyPutGet(t *testing.T) {
	for i := 0; i < 100; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp.Put(testKey, testValue)
		res, err := mp.Get(testKey)
		AssertEqualf(t, err, res, testValue, "get returned a wrong value")
	}
	mp.Clear()
}

func TestMapProxy_Remove(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.Remove(testKey)
	AssertEqualf(t, err, removed, testValue, "remove returned a wrong value")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(0), "Map size should be 0.")
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()

}
func TestMapProxy_RemoveIfSame(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.RemoveIfSame(testKey, "testinValue1")
	AssertEqualf(t, err, removed, false, "removeIfSame returned a wrong value")
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, true, "containsKey returned a wrong result")
	mp.Clear()
}
func TestMapProxy_PutTransient(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 100, time.Second)
	res, err := mp.Get(testKey)
	AssertEqualf(t, err, res, "nextValue", "putTransient failed")
	mp.Clear()

}
func TestMapProxy_PutTransientWhenExpire(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mp.PutTransient(testKey, "nextValue", 1, time.Millisecond)
	time.Sleep(5 * time.Second)
	res, err := mp.Get(testKey)
	AssertNilf(t, err, res, "putTransient failed")
	mp.Clear()

}
func TestMapProxy_ContainsKey(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsKey(testKey)
	AssertEqualf(t, err, found, true, "containsKey returned a wrong result")
	found, err = mp.ContainsKey("testingKey2")
	AssertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()
}
func TestMapProxy_ContainsValue(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsValue(testValue)
	AssertEqualf(t, err, found, true, "containsValue returned a wrong result")
	found, err = mp.ContainsValue("testingValue2")
	AssertEqualf(t, err, found, false, "containsValue returned a wrong result")
	mp.Clear()
}
func TestMapProxy_Clear(t *testing.T) {
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	err := mp.Clear()
	if err != nil {
		t.Fatal(err)
	} else {
		size, err := mp.Size()
		AssertEqualf(t, err, size, int32(0), "Map clear failed.")
	}
}

func TestMapProxy_Delete(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Delete("testingKey1")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(9), "Map Delete failed")
	mp.Clear()
}

func TestMapProxy_IsEmpty(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	empty, err := mp.IsEmpty()
	AssertEqualf(t, err, empty, false, "Map IsEmpty returned a wrong value")
	mp.Clear()
}

func TestMapProxy_Evict(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Evict("testingKey1")
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(9), "Map evict failed.")
	found, err := mp.ContainsKey("testingKey1")
	AssertEqualf(t, err, found, false, "Map evict failed.")
}
func TestMapProxy_EvictAll(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.EvictAll()
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(0), "Map evict failed.")
}
func TestMapProxy_Flush(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	err := mp.Flush()
	if err != nil {
		t.Fatal(err)
	}
	mp.Clear()
}
func TestMapProxy_IsLocked(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	err = mp.Lock("testingKey")
	if err != nil {
		t.Fatal(err)
	}
	locked, err = mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, true, "Key should be locked.")
	err = mp.Unlock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")

}
func TestMapProxy_LockWithLeaseTime(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	mp.LockWithLeaseTime("testingKey", 10, time.Millisecond)
	time.Sleep(5 * time.Second)
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
}
func TestMapProxy_TryLock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1, time.Second, 2, time.Second)
	AssertEqualf(t, err, ok, true, "Try Lock failed")
	time.Sleep(5 * time.Second)
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	mp.ForceUnlock("testingKey")

}
func TestMapProxy_ForceUnlock(t *testing.T) {
	mp.Put("testingKey", "testingValue")
	ok, err := mp.TryLockWithTimeoutAndLease("testingKey", 1, time.Second, 20, time.Second)
	AssertEqualf(t, err, ok, true, "Try Lock failed")
	mp.ForceUnlock("testingKey")
	locked, err := mp.IsLocked("testingKey")
	AssertEqualf(t, err, locked, false, "Key should not be locked.")
	mp.Unlock("testingKey")
}
func TestMapProxy_Replace(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.Replace("testingKey1", "testingValue2")
	AssertEqualf(t, err, replaced, "testingValue1", "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue2", "Map Replace failed.")
	mp.Clear()
}
func TestMapProxy_Size(t *testing.T) {
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	size, err := mp.Size()
	AssertEqualf(t, err, size, int32(10), "Map size returned a wrong value")
	mp.Clear()
}
func TestMapProxy_ReplaceIfSame(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue1", "testingValue2")
	AssertEqualf(t, err, replaced, true, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue2", "Map ReplaceIfSame failed.")
	mp.Clear()
}
func TestMapProxy_ReplaceIfSameWhenDifferent(t *testing.T) {
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue3", "testingValue2")
	AssertEqualf(t, err, replaced, false, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map ReplaceIfSame failed.")
	mp.Clear()
}
func TestMapProxy_Set(t *testing.T) {
	err := mp.Set("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}
func TestMapProxy_SetWithTtl(t *testing.T) {
	err := mp.SetWithTtl("testingKey1", "testingValue1", 0, time.Second)
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map SetWithTtl failed.")
	mp.SetWithTtl("testingKey1", "testingValue2", 1, time.Millisecond)
	time.Sleep(5 * time.Second)
	newValue, err = mp.Get("testingKey1")
	AssertNilf(t, err, newValue, "Map SetWithTtl failed.")
	mp.Clear()
}
func TestMapProxy_PutIfAbsent(t *testing.T) {
	_, err := mp.PutIfAbsent("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	AssertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_PutAll(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := mp.PutAll(&testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		entryList, err := mp.EntrySet()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range entryList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := testMap[key]
			if !found || expectedValue != value {
				t.Fatalf("Map PutAll failed")
			}
		}
	}
	mp.Clear()
}
func TestMapProxy_EntrySetWithPredicate(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	searchedMap := make(map[interface{}]interface{})
	values := []string{"value1", "wantedValue", "wantedValue", "value2", "value3", "wantedValue", "wantedValue", "value4", "value5", "wantedValue"}
	searchedMap["testingKey1"] = "wantedValue"
	searchedMap["testingKey2"] = "wantedValue"
	searchedMap["testingKey5"] = "wantedValue"
	searchedMap["testingKey6"] = "wantedValue"
	searchedMap["testingKey9"] = "wantedValue"
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = values[i]
	}
	err := mp.PutAll(&testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		entryList, err := mp.EntrySetWithPredicate(Sql("this == wantedValue"))
		if err != nil {
			t.Fatal(err)
		}
		if len(searchedMap) != len(entryList) {
			t.Fatalf("map EntrySetWithPredicate() failed")
		}
		for _, pair := range entryList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := searchedMap[key]
			if !found || expectedValue != value {
				t.Fatalf("map EntrySetWithPredicate() failed")
			}
		}
	}
	mp.Clear()
}
func TestMapProxy_GetAll(t *testing.T) {
	testMap := make(map[interface{}]interface{})
	for i := 0; i < 10; i++ {
		testMap["testingKey"+strconv.Itoa(i)] = "testingValue" + strconv.Itoa(i)
	}
	err := mp.PutAll(&testMap)
	if err != nil {
		t.Fatal(err)
	} else {
		keys := make([]interface{}, 0)
		for k, _ := range testMap {
			keys = append(keys, k)
		}
		valueList, err := mp.GetAll(keys)
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range valueList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := testMap[key]

			if !found || expectedValue != value {
				t.Fatalf("Map GetAll failed")
			}
		}

	}
	mp.Clear()
}
func TestMapProxy_GetEntryView(t *testing.T) {
	mp.Put("key", "value")
	mp.Get("key")
	mp.Put("key", "newValue")

	entryView, err := mp.GetEntryView("key")
	AssertEqualf(t, err, entryView.Hits(), int64(2), "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.EvictionCriteriaNumber(), int64(0), "Map GetEntryView returned a wrong view.")
	AssertEqualf(t, err, entryView.Version(), int64(1), "Map GetEntryView returned a wrong view.")

	mp.Clear()
}

type AddEntry struct {
	wg *sync.WaitGroup
}

func (addEntry *AddEntry) EntryAdded(event IEntryEvent) {
	addEntry.wg.Done()
}
func (addEntry *AddEntry) EntryUpdated(event IEntryEvent) {
	addEntry.wg.Done()
}
func (addEntry *AddEntry) EntryRemoved(event IEntryEvent) {
	addEntry.wg.Done()
}
func (addEntry *AddEntry) EntryEvicted(event IEntryEvent) {
	addEntry.wg.Done()
}
func (addEntry *AddEntry) EntryEvictAll(event IMapEvent) {
	addEntry.wg.Done()
}
func (addEntry *AddEntry) EntryClearAll(event IMapEvent) {
	addEntry.wg.Done()
}

func TestMapProxy_AddEntryListenerAdded(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key123", "value")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryAdded failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerUpdated(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("key1", "value")
	mp.Put("key1", "value")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryUpdated failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerEvicted(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Evict("test")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryEvicted failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerRemoved(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Remove("test")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryRemoved failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerEvictAll(t *testing.T) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.EvictAll()
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryEvictAll failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerClear(t *testing.T) {

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListener(entryAdded, true)
	AssertEqual(t, err, nil, nil)
	wg.Add(2)
	mp.Put("test", "key")
	mp.Clear()
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListener entryClear failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_AddEntryListenerToKey(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerToKey(entryAdded, "key1", true)
	AssertEqual(t, err, nil, nil)
	wg.Add(1)
	mp.Put("key1", "value1")
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "AddEntryListenerToKey failed")
	wg.Add(1)
	mp.Put("key2", "value1")
	timeout = WaitTimeout(wg, Timeout/20)
	AssertEqualf(t, nil, true, timeout, "AddEntryListenerToKey failed")
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}
func TestMapProxy_RemoveEntryListenerToKeyWithInvalidRegistrationId(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	entryAdded := &AddEntry{wg: wg}
	registrationId, err := mp.AddEntryListenerToKey(entryAdded, "key1", true)
	AssertEqual(t, err, nil, nil)
	invalidRegistrationId := "invalid"
	err = mp.RemoveEntryListener(&invalidRegistrationId)
	if _, ok := err.(*common.HazelcastKeyError); !ok {
		t.Fatal("remove entry listener to key with invalid registration id failed")
	}
	mp.RemoveEntryListener(registrationId)
	mp.Clear()
}

func TestMapProxy_ExecuteOnKey(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue)
	config.SerializationConfig.AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mpName := "testMap2"
	mp2, _ := client.GetMap(&mpName)
	testKey := "testingKey1"
	testValue := "testingValue"
	mp2.Put(testKey, testValue)
	value, err := mp2.ExecuteOnKey(testKey, processor)
	AssertEqualf(t, err, value, expectedValue, "ExecuteOnKey failed.")
	newValue, err := mp2.Get("testingKey1")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKey failed")
	mp.Clear()
	client.Shutdown()
}
func TestMapProxy_ExecuteOnKeys(t *testing.T) {

	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue)
	config.SerializationConfig.AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mpName := "testMap2"
	mp2, _ := client.GetMap(&mpName)
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp2.Put(testKey, testValue)
	}
	keys := make([]interface{}, 2)
	keys[0] = "testingKey1"
	keys[1] = "testingKey2"
	result, err := mp2.ExecuteOnKeys(keys, processor)
	AssertEqualf(t, err, len(result), 2, "ExecuteOnKeys failed.")
	newValue, err := mp2.Get("testingKey1")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKeys failed")
	newValue, err = mp2.Get("testingKey2")
	AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnKeys failed")
	mp2.Clear()
	client.Shutdown()
}
func TestMapProxy_ExecuteOnEntries(t *testing.T) {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue)
	config.SerializationConfig.AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mpName := "testMap2"
	mp2, _ := client.GetMap(&mpName)
	for i := 0; i < 10; i++ {
		testKey := "testingKey" + strconv.Itoa(i)
		testValue := "testingValue" + strconv.Itoa(i)
		mp2.Put(testKey, testValue)
	}
	result, err := mp2.ExecuteOnEntries(processor)
	for _, pair := range result {
		AssertEqualf(t, err, pair.Value(), expectedValue, "ExecuteOnEntries failed.")
		newValue, err := mp2.Get(pair.Key())
		AssertEqualf(t, err, newValue, expectedValue, "ExecuteOnEntries failed")
	}

	mp.Clear()
	client.Shutdown()
}
func TestMapProxy_Destroy(t *testing.T) {
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	mapName := "myMap"
	mp.Destroy()
	mp, _ := client.GetMap(&mapName)
	res, err := mp.Get(testKey)
	AssertNilf(t, err, res, "get returned a wrong value")
}

type simpleEntryProcessor struct {
	classId           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor(value string) *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classId: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryId: 66, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryId            int32
}

func (identifiedFactory *identifiedFactory) Create(id int32) IdentifiedDataSerializable {
	if id == identifiedFactory.simpleEntryProcessor.classId {
		return &simpleEntryProcessor{classId: 1}
	} else {
		return nil
	}
}

func (simpleEntryProcessor *simpleEntryProcessor) ReadData(input DataInput) error {
	var err error
	simpleEntryProcessor.value, err = input.ReadUTF()
	return err
}

func (simpleEntryProcessor *simpleEntryProcessor) WriteData(output DataOutput) {
	output.WriteUTF(simpleEntryProcessor.value)
}

func (simpleEntryProcessor *simpleEntryProcessor) FactoryId() int32 {
	return simpleEntryProcessor.identifiedFactory.factoryId
}

func (simpleEntryProcessor *simpleEntryProcessor) ClassId() int32 {
	return simpleEntryProcessor.classId
}
