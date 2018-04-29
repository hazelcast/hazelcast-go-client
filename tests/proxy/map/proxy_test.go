package _map

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

func TestProxy_Destroy(t *testing.T) {
	name := "testMap"
	serviceName := common.ServiceNameMap
	testMap, err := client.DistributedObject(serviceName, name)
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
	serviceName := common.ServiceNameMap
	mp, _ := client.DistributedObject(serviceName, name)
	mp2, _ := client.DistributedObject(serviceName, name)

	if !reflect.DeepEqual(mp, mp2) {
		t.Error("DistributedObject() works wrong")
	}
}
