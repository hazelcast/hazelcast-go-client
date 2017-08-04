package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	. "github.com/hazelcast/go-client/internal/serialization"
)


type proxy struct {
	client      HazelcastClient
	serviceName string
	name        string
}

func (proxy *proxy) InvokeOnKey(request *ClientMessage, keyData *Data) (*ClientMessage, error) {
	return proxy.client.InvocationService.InvokeOnKeyOwner(request, keyData).Result()
}

func (proxy *proxy) ToObject(data *Data) (interface{}, error) {
	return proxy.client.SerializationService.ToObject(data)
}

func (proxy *proxy) ToData(object interface{}) (*Data, error) {
	return proxy.client.SerializationService.ToData(object)
}
