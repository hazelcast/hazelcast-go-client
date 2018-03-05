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
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/common/collection"
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

func (proxy *proxy) validateAndSerialize(arg1 interface{}) (arg1Data *Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastNilPointerError(NIL_ARG_IS_NOT_ALLOWED, nil)
	}
	arg1Data, err = proxy.ToData(arg1)
	return
}

func (proxy *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data *Data, arg2Data *Data, err error) {
	if arg1 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(NIL_ARG_IS_NOT_ALLOWED, nil)
	}
	if arg2 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(NIL_ARG_IS_NOT_ALLOWED, nil)
	}
	arg1Data, err = proxy.ToData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = proxy.ToData(arg2)
	return
}

func (proxy *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data *Data, arg2Data *Data, arg3Data *Data, err error) {
	if arg1 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(NIL_ARG_IS_NOT_ALLOWED, nil)
	}
	if arg2 == nil || arg3 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(NIL_ARG_IS_NOT_ALLOWED, nil)
	}
	arg1Data, err = proxy.ToData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = proxy.ToData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = proxy.ToData(arg3)
	return
}

func (proxy *proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data *Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastSerializationError(NIL_PREDICATE_IS_NOT_ALLOWED, nil)
	}
	arg1Data, err = proxy.ToData(arg1)
	return
}

func (proxy *proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []*Data, err error) {
	if elements == nil {
		return nil, core.NewHazelcastSerializationError(NIL_SLICE_IS_NOT_ALLOWED, nil)
	}
	elementsData, err = collection.ObjectToDataCollection(elements, proxy.client.SerializationService)
	return
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

func (proxy *proxy) DecodeToObjectAndError(responseMessage *ClientMessage, inputError error,
	decodeFunc func(*ClientMessage) func() *Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return proxy.ToObject(decodeFunc(responseMessage)())
}

func (proxy *proxy) DecodeToBoolAndError(responseMessage *ClientMessage, inputError error,
	decodeFunc func(*ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (proxy *proxy) DecodeToInterfaceSliceAndError(responseMessage *ClientMessage, inputError error,
	decodeFunc func(*ClientMessage) func() []*Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectCollection(decodeFunc(responseMessage)(), proxy.client.SerializationService)
}

func (proxy *proxy) DecodeToPairSliceAndError(responseMessage *ClientMessage, inputError error,
	decodeFunc func(*ClientMessage) func() []*Pair) (response []core.IPair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectPairCollection(decodeFunc(responseMessage)(), proxy.client.SerializationService)
}

func (proxy *proxy) DecodeToInt32AndError(responseMessage *ClientMessage, inputError error,
	decodeFunc func(*ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
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

func (parSpecProxy *partitionSpecificProxy) Invoke(request *ClientMessage) (*ClientMessage, error) {
	return parSpecProxy.InvokeOnPartition(request, parSpecProxy.partitionId)
}

func (proxy *proxy) createOnItemEvent(listener interface{}) func(itemData *Data, uuid *string, eventType int32) {
	return func(itemData *Data, uuid *string, eventType int32) {
		var item interface{}
		item, _ = proxy.ToObject(itemData)
		member := proxy.client.ClusterService.GetMemberByUuid(*uuid)
		itemEvent := NewItemEvent(proxy.name, item, eventType, member.(*Member))
		if eventType == ITEM_ADDED {
			if _, ok := listener.(core.ItemAddedListener); ok {
				listener.(core.ItemAddedListener).ItemAdded(itemEvent)
			}
		} else if eventType == ITEM_REMOVED {
			if _, ok := listener.(core.ItemRemovedListener); ok {
				listener.(core.ItemRemovedListener).ItemRemoved(itemEvent)
			}
		}
	}
}
