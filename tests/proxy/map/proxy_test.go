package _map

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func TestProxy_Destroy(t *testing.T) {
	name := "testMap"
	serviceName := common.ServiceNameMap
	testMap, err := client.GetDistributedObject(serviceName, name)
	res, err := testMap.Destroy()

	if res != true || err != nil {
		t.Error("Destroy() works wrong")
	}

	res, err = testMap.Destroy()

	if res != false || err != nil {
		t.Error("Destroy() works wrong")
	}
}

func TestProxy_GetDistributedObject(t *testing.T) {
	name := "testMap"
	serviceName := common.ServiceNameMap
	mp, _ := client.GetDistributedObject(serviceName, name)
	mp2, _ := client.GetDistributedObject(serviceName, name)

	if !reflect.DeepEqual(mp, mp2) {
		t.Error("GetDistributedObject() works wrong")
	}
}
