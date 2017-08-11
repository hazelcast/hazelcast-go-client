package tests

import (
	//"fmt"
	"github.com/hazelcast/go-client"
	"testing"
	//"time"
	//"strconv"
	"bytes"
	"github.com/hazelcast/go-client/internal/serialization"
	"strconv"
)
//Rc is not in use currenly. In order to run these tests open a server manually.
func TestMapProxy_SinglePutGet(t *testing.T) {
	client :=hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	mp.Put("testingkey","testingvalue")
	x,err:=mp.Get("testingkey")
	if err != nil{
		t.Error(err)
	}else {
		//Since serialization is not completed for string, comparing interfaces looks ugly now.
		if bytes.Compare(x.(*serialization.Data).Payload, []byte("testingvalue")) !=0 {
			t.Errorf("get returned a wrong value")
		}
	}
}
func TestMapProxy_ManyPutGet(t *testing.T){
	client :=hazelcast.NewHazelcastClient()
	mapName := "myMap"
	mp := client.GetMap(&mapName)
	for i := 0 ;i < 10000 ;i++ {
		mp.Put("testingkey"+strconv.Itoa(i),"testingvalue"+strconv.Itoa(i))
		x,err:=mp.Get("testingkey"+strconv.Itoa(i))
		if err != nil{
			t.Error(err)
		}else {
			//Since serialization is not completed for string, comparing interfaces looks ugly now.
			if bytes.Compare(x.(*serialization.Data).Payload, []byte("testingvalue"+strconv.Itoa(i))) !=0 {
				t.Errorf("get returned a wrong value")
			}
		}
	}
}

