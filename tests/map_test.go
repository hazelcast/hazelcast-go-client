package tests

import (
	"bytes"
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/internal/serialization"
	. "github.com/hazelcast/go-client/rc"
	"testing"
)

const DEFAULT_XML_CONFIG string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.9.xsd\" xmlns=\"http://www.hazelcast.com/schema/config\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></hazelcast>"

//Rc is not in use currenly. In order to run these tests open a server manually.
func TestMapProxy_SinglePutGet(t *testing.T) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		t.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	client := hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingkey", "testingvalue")
	x, err := mp.Get("testingkey")
	if err != nil {
		t.Error(err)
	} else {
		//Since serialization is not completed for string, comparing interfaces looks ugly now.
		// TODO serialization is completed for string now
		if bytes.Compare(x.(*serialization.Data).Payload, []byte("testingvalue")) != 0 {
			t.Errorf("get returned a wrong value")
		}
	}
	remoteController.ShutdownCluster(cluster.ID)
}
