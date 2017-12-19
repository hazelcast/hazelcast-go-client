// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
