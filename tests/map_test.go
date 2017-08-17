package tests

import (
	"github.com/hazelcast/go-client"
	. "github.com/hazelcast/go-client/rc"
	"log"
	"strconv"
	"testing"
)

const DEFAULT_XML_CONFIG string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.9.xsd\" xmlns=\"http://www.hazelcast.com/schema/config\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></hazelcast>"

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
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
	if err != nil {
		t.Error(err)
	} else {
		if res != testValue {
			t.Errorf("get returned a wrong value")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if removed != testValue {
			t.Errorf("remove returned a wrong value")
		}
	}
	size, err := mp.Size()
	if err != nil {
		t.Error(err)
	} else {
		if size != 0 {
			t.Errorf("Map size should be 0.")
		}
	}
	found, err := mp.ContainsKey(testKey)
	if err != nil {
		t.Error(err)
	} else {
		if found {
			t.Errorf("containsKey returned a wrong result")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if !found {
			t.Errorf("containsKey returned a wrong result")
		}
	}
	found, err = mp.ContainsKey("testingKey2")
	if err != nil {
		t.Error(err)
	} else {
		if found {
			t.Errorf("containsKey returned a wrong result")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if !found {
			t.Errorf("containsValue returned a wrong result")
		}
	}
	found, err = mp.ContainsValue("testingValue2")
	if err != nil {
		t.Error(err)
	} else {
		if found {
			t.Errorf("containsValue returned a wrong result")
		}
	}
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
		t.Error(err)
	} else {
		size, err := mp.Size()
		if err != nil {
			t.Error(err)
		} else {
			if size != 0 {
				t.Errorf("Map clear failed.")
			}
		}
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
	if err != nil {
		t.Error(err)
	} else {
		if size != 9 {
			t.Errorf("Map Delete failed")
		}
	}
}

func TestMapProxy_IsEmpty(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0; i < 10; i++ {
		mp.Put("testingKey"+strconv.Itoa(i), "testingValue"+strconv.Itoa(i))
	}
	empty, err := mp.IsEmpty()
	if err != nil {
		t.Error(err)
	} else {
		if empty {
			t.Errorf("Map IsEmpty returned a wrong value")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if size != 9 {
			t.Errorf("Map evict failed.")
		}
	}
	found, err := mp.ContainsKey("testingKey1")
	if err != nil {
		t.Error(err)
	} else {
		if found {
			t.Errorf("Map evict failed.")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if size != 0 {
			t.Errorf("Map evict failed.")
		}
	}
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
		t.Error(err)
	}
	mp.Clear()
}
func TestMapProxy_IsLocked(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey", "testingValue")
	locked, err := mp.IsLocked("testingKey")
	if err != nil {
		t.Error(err)
	} else {
		if locked {
			t.Errorf("Key should not be locked.")
		}
	}
	err = mp.Lock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	if err != nil {
		t.Error(err)
	} else {
		if !locked {
			t.Errorf("Key should be locked.")
		}
	}
	err = mp.UnLock("testingKey")
	if err != nil {
		t.Error(err)
	}
	locked, err = mp.IsLocked("testingKey")
	if err != nil {
		t.Error(err)
	} else {
		if locked {
			t.Errorf("Key should not be locked.")
		}
	}
}
func TestMapProxy_Replace(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.Replace("testingKey1", "testingValue2")
	if err != nil {
		t.Error(err)
	} else {
		if replaced != "testingValue1" {
			t.Errorf("Map Replace returned wrong old value.")
		}
	}
	newValue, err := mp.Get("testingKey1")
	if err != nil {
		t.Error(err)
	} else {
		if newValue != "testingValue2" {
			t.Errorf("Map Replace failed.")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if size != 10 {
			t.Errorf("Map size returned a wrong value")
		}
	}
	mp.Clear()
}
func TestMapProxy_ReplaceIfSame(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue1", "testingValue2")
	if err != nil {
		t.Error(err)
	} else {
		if !replaced {
			t.Errorf("Map Replace returned wrong old value.")
		}
	}
	newValue, err := mp.Get("testingKey1")
	if err != nil {
		t.Error(err)
	} else {
		if newValue != "testingValue2" {
			t.Errorf("Map ReplaceIfSame failed.")
		}
	}
	mp.Clear()
}
func TestMapProxy_ReplaceIfSameWhenDifferent(t *testing.T) {
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingKey1", "testingValue1")
	replaced, err := mp.ReplaceIfSame("testingKey1", "testingValue3", "testingValue2")
	if err != nil {
		t.Error(err)
	} else {
		if replaced {
			t.Errorf("Map Replace returned wrong old value.")
		}
	}
	newValue, err := mp.Get("testingKey1")
	if err != nil {
		t.Error(err)
	} else {
		if newValue != "testingValue1" {
			t.Errorf("Map ReplaceIfSame failed.")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if newValue != "testingValue1" {
			t.Errorf("Map Set failed.")
		}
	}
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
	if err != nil {
		t.Error(err)
	} else {
		if newValue != "testingValue1" {
			t.Errorf("Map Set failed.")
		}
	}
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
		t.Error(err)
	} else {
		entryList, err := mp.EntrySet()
		if err != nil {
			t.Error(err)
		}
		for _, pair := range entryList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := testMap[key]
			if !found || expectedValue != value {
				t.Errorf("Map PutAll failed")
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
		t.Error(err)
	} else {
		keys := make([]interface{}, 0)
		for k, _ := range testMap {
			keys = append(keys, k)
		}
		valueList, err := mp.GetAll(keys)
		if err != nil {
			t.Error(err)
		}
		for _, pair := range *valueList {
			key := pair.Key()
			value := pair.Value()
			expectedValue, found := testMap[key]

			if !found || expectedValue != value {
				t.Errorf("Map GetAll failed")
			}
		}

	}
	mp.Clear()
}
