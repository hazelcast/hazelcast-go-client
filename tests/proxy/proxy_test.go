package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"reflect"
	"testing"
)

func TestProxy_Destroy(t *testing.T) {
	name := "testMap"
	serviceName := common.SERVICE_NAME_MAP
	testMap, err := client.GetDistributedObject(serviceName, name)
	res, err := testMap.Destroy()

	if res != true || err != nil {
		t.Errorf("Destroy() works wrong")
	}

	res, err = testMap.Destroy()

	if res != false || err != nil {
		t.Errorf("Destroy() works wrong")
	}
}

func TestProxy_GetDistributedObject(t *testing.T) {
	name := "testMap"
	serviceName := common.SERVICE_NAME_MAP
	mp, _ := client.GetDistributedObject(serviceName, name)
	mp2, _ := client.GetDistributedObject(serviceName, name)

	if !reflect.DeepEqual(mp, mp2) {
		t.Errorf("GetDistributedObject() works wrong")
	}
}
