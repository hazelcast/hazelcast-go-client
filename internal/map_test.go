package internal

import (
	"fmt"
	"github.com/hazelcast/go-client/config"
	"testing"
	"time"
)

func TestMapProxy_Put(t *testing.T) {
	mp := MapProxy{}
	mp.client = *NewHazelcastClient(config.NewClientConfig())
	mp.Put("abc", "dce")

	//x,_ := mp.Get("asdsa")
	time.Sleep(time.Duration(3 * time.Second))
	for k, v := range mp.client.PartitionService.partitions {
		fmt.Println(k, " ", v)
	}
	//fmt.Println(x)
}
