// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type proxy struct {
	client      *HazelcastClient
	serviceName *string
	name        *string
}

func (proxy *proxy) Destroy() (bool, error) {
	return proxy.client.ProxyManager.destroyProxy(proxy.serviceName, proxy.name)
}
func (proxy *proxy) isSmart() bool {
	return proxy.client.ClientConfig.ClientNetworkConfig().IsSmartRouting()
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

func (proxy *proxy) EncodeInvoke(codec Codec, codecArguments ...interface{}) (parameter interface{}, err error) {
	request := codec.EncodeRequest(codecArguments...)
	responseMessage, err := proxy.client.InvocationService.InvokeOnRandomTarget(request).Result()
	if err != nil {
		return nil, err
	}
	return codec.DecodeResponse(responseMessage, proxy.ToObject)
}

func (proxy *proxy) EncodeInvokeOnKey(codec Codec, keyData *Data, codecArguments ...interface{}) (interface{}, error) {
	partitionId := proxy.client.PartitionService.GetPartitionId(keyData)
	return proxy.EncodeInvokeOnPartition(codec, partitionId, codecArguments...)
}

func (proxy *proxy) EncodeInvokeOnPartition(codec Codec, partitionId int32, codecArguments ...interface{}) (parameter interface{}, err error) {
	request := codec.EncodeRequest(codecArguments...)
	responseMessage, err := proxy.client.InvocationService.InvokeOnPartitionOwner(request, partitionId).Result()
	if err != nil {
		return nil, err
	}
	return codec.DecodeResponse(responseMessage, proxy.ToObject)
}

func (proxy *proxy) ToObject(data *Data) (interface{}, error) {
	return proxy.client.SerializationService.ToObject(data)
}

func (proxy *proxy) ToData(object interface{}) (*Data, error) {
	return proxy.client.SerializationService.ToData(object)
}

type partitionSpecificProxy struct {
	*proxy
	partitionId int32
}

func newPartitionSpecificProxy(client *HazelcastClient, serviceName *string, name *string) (*partitionSpecificProxy, error) {
	var err error
	parSpecProxy := &partitionSpecificProxy{proxy: &proxy{client, serviceName, name}}
	parSpecProxy.partitionId, err = parSpecProxy.client.PartitionService.GetPartitionIdWithKey(parSpecProxy.PartitionKey())
	return parSpecProxy, err

}

func (parSpecProxy *partitionSpecificProxy) EncodeInvoke(request *ClientMessage) (*ClientMessage, error) {
	return parSpecProxy.client.InvocationService.InvokeOnPartitionOwner(request, parSpecProxy.partitionId).Result()
}
