package _map

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

func TestProxy_Destroy(t *testing.T) {
	name := "testMap"
	serviceName := bufutil.ServiceNameMap
	testMap, err := client.GetDistributedObject(serviceName, name)
	assert.ErrorNil(t, err)
	res, err := testMap.Destroy()

	if !res || err != nil {
		t.Error("Destroy() works wrong")
	}

	res, err = testMap.Destroy()

	if res || err != nil {
		t.Error("Destroy() works wrong")
	}
}

func TestProxy_GetDistributedObject(t *testing.T) {
	name := "testMap"
	serviceName := bufutil.ServiceNameMap
	mp, _ := client.GetDistributedObject(serviceName, name)
	mp2, _ := client.GetDistributedObject(serviceName, name)

	if !reflect.DeepEqual(mp, mp2) {
		t.Error("GetDistributedObject() works wrong")
	}
}
