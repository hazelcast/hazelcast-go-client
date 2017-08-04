package proxy

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type proxy struct {
	client      ClientContext
	serviceName string
	name        string
}

func (proxy *proxy) InvokeOnKey(request *ClientMessage, keyData *Data) <-chan *ClientMessage {


	//resultChan := connection.Send(request)
	return make(chan *ClientMessage)
}

func (proxy *proxy) ToObject(data *Data) interface{} {
	proxy.client.
	return nil
}

func (proxy *proxy) ToData(object interface{}) *Data {

	return &Data{}
}
