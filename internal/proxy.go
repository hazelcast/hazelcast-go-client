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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/common/collection"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	threadId     = 1
	ttlUnlimited = 0
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

func (proxy *proxy) validateAndSerialize(arg1 interface{}) (arg1Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = proxy.toData(arg1)
	return
}

func (proxy *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data *serialization.Data, arg2Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	if arg2 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = proxy.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = proxy.toData(arg2)
	return
}

func (proxy *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data *serialization.Data, arg2Data *serialization.Data, arg3Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	if arg2 == nil || arg3 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = proxy.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = proxy.toData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = proxy.toData(arg3)
	return
}

func (proxy *proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastSerializationError(common.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = proxy.toData(arg1)
	return
}

func (proxy *proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []*serialization.Data, err error) {
	if elements == nil {
		return nil, core.NewHazelcastSerializationError(common.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = collection.ObjectToDataCollection(elements, proxy.client.SerializationService)
	return
}

func (proxy *proxy) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*protocol.Pair, error) {
	if entries == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilMapIsNotAllowed, nil)
	}
	partitions := make(map[int32][]*protocol.Pair)
	for key, value := range entries {
		keyData, valueData, err := proxy.validateAndSerialize2(key, value)
		if err != nil {
			return nil, err
		}
		pair := protocol.NewPair(keyData, valueData)
		partitionId := proxy.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], pair)
	}
	return partitions, nil
}

func (proxy *proxy) invokeOnKey(request *protocol.ClientMessage, keyData *serialization.Data) (*protocol.ClientMessage, error) {
	return proxy.client.InvocationService.invokeOnKeyOwner(request, keyData).Result()
}

func (proxy *proxy) invokeOnRandomTarget(request *protocol.ClientMessage) (*protocol.ClientMessage, error) {
	return proxy.client.InvocationService.invokeOnRandomTarget(request).Result()
}

func (proxy *proxy) invokeOnPartition(request *protocol.ClientMessage, partitionId int32) (*protocol.ClientMessage, error) {
	return proxy.client.InvocationService.invokeOnPartitionOwner(request, partitionId).Result()
}

func (proxy *proxy) invokeOnAddress(request *protocol.ClientMessage, address *protocol.Address) (*protocol.ClientMessage, error) {
	return proxy.client.InvocationService.invokeOnTarget(request, address).Result()
}

func (proxy *proxy) toObject(data *serialization.Data) (interface{}, error) {
	return proxy.client.SerializationService.ToObject(data)
}

func (proxy *proxy) toData(object interface{}) (*serialization.Data, error) {
	return proxy.client.SerializationService.ToData(object)
}

func (proxy *proxy) decodeToObjectAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() *serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return proxy.toObject(decodeFunc(responseMessage)())
}

func (proxy *proxy) decodeToBoolAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (proxy *proxy) decodeToInterfaceSliceAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() []*serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectCollection(decodeFunc(responseMessage)(), proxy.client.SerializationService)
}

func (proxy *proxy) decodeToPairSliceAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() []*protocol.Pair) (response []core.IPair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectPairCollection(decodeFunc(responseMessage)(), proxy.client.SerializationService)
}

func (proxy *proxy) decodeToInt32AndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (proxy *proxy) decodeToInt64AndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() int64) (response int64, err error) {
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
	parSpecProxy.partitionId, err = parSpecProxy.client.PartitionService.getPartitionIdWithKey(parSpecProxy.PartitionKey())
	return parSpecProxy, err

}

func (parSpecProxy *partitionSpecificProxy) invoke(request *protocol.ClientMessage) (*protocol.ClientMessage, error) {
	return parSpecProxy.invokeOnPartition(request, parSpecProxy.partitionId)
}

func (proxy *proxy) createOnItemEvent(listener interface{}) func(itemData *serialization.Data, uuid *string, eventType int32) {
	return func(itemData *serialization.Data, uuid *string, eventType int32) {
		var item interface{}
		item, _ = proxy.toObject(itemData)
		member := proxy.client.ClusterService.GetMemberByUuid(*uuid)
		itemEvent := protocol.NewItemEvent(proxy.name, item, eventType, member.(*protocol.Member))
		if eventType == common.ItemAdded {
			if _, ok := listener.(core.ItemAddedListener); ok {
				listener.(core.ItemAddedListener).ItemAdded(itemEvent)
			}
		} else if eventType == common.ItemRemoved {
			if _, ok := listener.(core.ItemRemovedListener); ok {
				listener.(core.ItemRemovedListener).ItemRemoved(itemEvent)
			}
		}
	}
}
