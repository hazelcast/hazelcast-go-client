package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"testing"
)

func TestMultiMapProxy_Name(t *testing.T) {
	name := "myMultiMap"
	if name != mmp.Name() {
		t.Errorf("Name() failed")
	}
}

func TestMultiMapProxy_ServiceName(t *testing.T) {
	serviceName := common.SERVICE_NAME_MULTI_MAP
	if serviceName != mmp.ServiceName() {
		t.Errorf("ServiceName() failed")
	}
}

func TestMultiMapProxy_PartitionKey(t *testing.T) {
	name := "myMultiMap"
	if name != mmp.PartitionKey() {
		t.Errorf("PartitionKey() failed")
	}
}

func TestMultiMapProxy_Destroy(t *testing.T) {

}

func TestMultiMapProxy_Put(t *testing.T) {

}

func TestMultiMapProxy_PutAll(t *testing.T) {

}

func TestMultiMapProxy_Get(t *testing.T) {

}

func TestMultiMapProxy_(t *testing.T) {

}
