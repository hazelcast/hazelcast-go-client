package internal

import (
	"testing"
	"github.com/hazelcast/go-client/config"

	"fmt"
)

func TestMapProxy_Put(t *testing.T) {
	mp := MapProxy{}
	mp.client = *NewHazelcastClient(config.NewClientConfig())
	mp.Put("abc","dce")
	for i := 0; i<1000;i++{
		 mp.Put("asd","Asd")
	}
	x,_ := mp.Get("asdsa")
	fmt.Println(x)
}
