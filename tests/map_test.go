package tests

import (
	"github.com/hazelcast/go-client"
	. "github.com/hazelcast/go-client/rc"
	"log"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", nil)
	remoteController.StartMember(cluster.ID)
	m.Run()
	remoteController.ShutdownCluster(cluster.ID)
}
func TestMapProxy_SinglePutGet(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	res, err := mp.Get(testKey)
	assertEqualf(t, err, res, testValue, "get returned a wrong value")
	mp.Clear()
}

func TestMapProxy_Remove(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	removed, err := mp.Remove(testKey)
	assertEqualf(t, err, removed, testValue, "remove returned a wrong value")
	size, err := mp.Size()
	assertEqualf(t, err, size, int32(0), "Map size should be 0.")
	found, err := mp.ContainsKey(testKey)
	assertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()

}
func TestMapProxy_ContainsKey(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsKey(testKey)
	assertEqualf(t, err, found, true, "containsKey returned a wrong result")
	found, err = mp.ContainsKey("testingKey2")
	assertEqualf(t, err, found, false, "containsKey returned a wrong result")
	mp.Clear()
}
func TestMapProxy_ContainsValue(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	found, err := mp.ContainsValue(testValue)
	assertEqualf(t, err, found, true, "containsValue returned a wrong result")
	found, err = mp.ContainsValue("testingValue2")
	assertEqualf(t, err, found, false, "containsValue returned a wrong result")
	mp.Clear()
}
func TestMapProxy_Clear(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	testKey := "testingKey1"
	testValue := "testingValue"
	mp.Put(testKey, testValue)
	err := mp.Clear()
	if err != nil {
		t.Fatal(err)
	} else {
		size, err := mp.Size()
		assertEqualf(t, err, size, int32(0), "Map clear failed.")
	}
}

func TestMapProxy_Delete(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Delete("testingKey1")
	size, err := mp.Size()
	assertEqualf(t, err, size, int32(9), "Map Delete failed")
	mp.Clear()
}

func TestMapProxy_IsEmpty(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	empty, err := mp.IsEmpty()
	assertEqualf(t, err, empty, false, "Map IsEmpty returned a wrong value")
	mp.Clear()
}

func TestMapProxy_Evict(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.Evict("testingKey1")
	size, err := mp.Size()
	assertEqualf(t, err, size, int32(9), "Map evict failed.")
	found, err := mp.ContainsKey("testingKey1")
	assertEqualf(t, err, found, false, "Map evict failed.")
}
func TestMapProxy_EvictAll(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	mp.EvictAll()
	size, err := mp.Size()
	assertEqualf(t, err, size, int32(0), "Map evict failed.")
}
func TestMapProxy_Flush(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
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
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey", "testingValue")
	locked, err := mp.IsLocked("testingKey")
	assertEqualf(t, err, locked, false, "Key should not be locked.")
	err = mp.Lock("testingKey")
	if err != nil {
		t.Fatal(err)
	}
	locked, err = mp.IsLocked("testingKey")
	assertEqualf(t, err, locked, true, "Key should be locked.")
	err = mp.UnLock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	assertEqualf(t, err, locked, false, "Key should not be locked.")
}
func TestMapProxy_Replace(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.Replace("testingKey1", "testingValue2")
	assertEqualf(t, err, replaced, "testingValue1", "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	assertEqualf(t, err, newValue, "testingValue2", "Map Replace failed.")
	mp.Clear()
}
func TestMapProxy_Size(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	size, err := mp.Size()
	assertEqualf(t, err, size, int32(10), "Map size returned a wrong value")
	mp.Clear()
}
func TestMapProxy_ReplaceIfSame(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue1", "testingValue2")
	assertEqualf(t, err, replaced, true, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	assertEqualf(t, err, newValue, "testingValue2", "Map ReplaceIfSame failed.")
	mp.Clear()
}
func TestMapProxy_ReplaceIfSameWhenDifferent(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue3", "testingValue2")
	assertEqualf(t, err, replaced, false, "Map Replace returned wrong old value.")
	newValue, err := mp.Get("testingKey1")
	assertEqualf(t, err, newValue, "testingValue1", "Map ReplaceIfSame failed.")
	mp.Clear()
}
func TestMapProxy_Set(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	err := mp.Set("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	assertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}
func TestMapProxy_PutIfAbsent(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	_, err := mp.PutIfAbsent("testingKey1", "testingValue1")
	if err != nil {
		t.Error(err)
	}
	newValue, err := mp.Get("testingKey1")
	assertEqualf(t, err, newValue, "testingValue1", "Map Set failed.")
	mp.Clear()
}

func TestMapProxy_PutAll(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
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
func TestMapProxy_GetAll(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
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
		for _, pair := range *valueList {
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
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("key", "value")
	mp.Get("key")
	mp.Put("key", "newValue")

	entryView, err := mp.GetEntryView("key")
	assertEqualf(t, err, entryView.Hits(), int64(2), "Map GetEntryView returned a wrong view.")
	assertEqualf(t, err, entryView.EvictionCriteriaNumber(), int64(0), "Map GetEntryView returned a wrong view.")
	assertEqualf(t, err, entryView.Version(), int64(1), "Map GetEntryView returned a wrong view.")

	mp.Clear()
}
