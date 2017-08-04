package proxy

import (
	"github.com/hazelcast/go-client"
	. "github.com/hazelcast/go-client/internal"
)

type proxy struct {
	client      HazelcastClient
	serviceName string
	name        string
}

func (proxy *proxy) InvokeOnKey(request *ClientMessage, keyData *hazelcast.Data) <-chan *ClientMessage {


	//resultChan := connection.Send(request)
	return make(chan *ClientMessage)
}

func (proxy *proxy) ToObject(data *hazelcast.Data) interface{} {
	return nil
}

func (proxy *proxy) ToData(object interface{}) *hazelcast.Data {

	return &hazelcast.Data{}
}
