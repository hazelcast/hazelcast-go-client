package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type proxy struct {
	client      *HazelcastClient
	serviceName *string
	name        *string
}

func (proxy *proxy) Destroy() (bool,error) {
	return proxy.client.ProxyManager.destroyProxy(proxy.serviceName, proxy.name)
}

func (proxy *proxy) Name() string {
	return *proxy.name
}
func (proxy *proxy) PartitionKey() string {
	return *proxy.name
}
func (proxy *proxy) ServiceName() string {
	return *proxy.serviceName
}

func (proxy *proxy) InvokeOnKey(request *ClientMessage, keyData *Data) (*ClientMessage, error) {
	return proxy.client.InvocationService.InvokeOnKeyOwner(request, keyData).Result()
}
func (proxy *proxy) InvokeOnRandomTarget(request *ClientMessage) (*ClientMessage, error) {
	return proxy.client.InvocationService.InvokeOnRandomTarget(request).Result()
}
func (proxy *proxy) InvokeOnPartition(request *ClientMessage, partitionId int32) (*ClientMessage, error) {
	return proxy.client.InvocationService.InvokeOnPartitionOwner(request, partitionId).Result()
}
func (proxy *proxy) ToObject(data *Data) (interface{}, error) {
	return proxy.client.SerializationService.ToObject(data)
}

func (proxy *proxy) ToData(object interface{}) (*Data, error) {
	return proxy.client.SerializationService.ToData(object)
}
