package proxy

import (
	. "github.com/hazelcast/go-client/internal"
	. "github.com/hazelcast/go-client"
)


type Map struct {
	proxy
}

func (imap *Map) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	var request ClientMessage
	var keyData Data
	responseMessage :=<-imap.InvokeOnKey(&request, &keyData)
	responseMessage.



}

func (imap *Map) Get(key interface{}) (value interface{}, err error) {


}
