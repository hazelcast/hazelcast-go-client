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
	threadID     = 1
	ttlUnlimited = 0
)

type proxy struct {
	client      *HazelcastClient
	serviceName *string
	name        *string
}

func (p *proxy) Destroy() (bool, error) {
	return p.client.ProxyManager.destroyProxy(p.serviceName, p.name)
}

func (p *proxy) isSmart() bool {
	return p.client.ClientConfig.ClientNetworkConfig().IsSmartRouting()
}

func (p *proxy) Name() string {
	return *p.name
}

func (p *proxy) PartitionKey() string {
	return *p.name
}

func (p *proxy) ServiceName() string {
	return *p.serviceName
}

func (p *proxy) validateAndSerialize(arg1 interface{}) (arg1Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data *serialization.Data,
	arg2Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	if arg2 == nil {
		return nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	return
}

func (p *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data *serialization.Data,
	arg2Data *serialization.Data, arg3Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	if arg2 == nil || arg3 == nil {
		return nil, nil, nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = p.toData(arg3)
	return
}

func (p *proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data *serialization.Data, err error) {
	if arg1 == nil {
		return nil, core.NewHazelcastSerializationError(common.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []*serialization.Data, err error) {
	if elements == nil {
		return nil, core.NewHazelcastSerializationError(common.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = collection.ObjectToDataCollection(elements, p.client.SerializationService)
	return
}

func (p *proxy) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*protocol.Pair, error) {
	if entries == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilMapIsNotAllowed, nil)
	}
	partitions := make(map[int32][]*protocol.Pair)
	for key, value := range entries {
		keyData, valueData, err := p.validateAndSerialize2(key, value)
		if err != nil {
			return nil, err
		}
		pair := protocol.NewPair(keyData, valueData)
		partitionID := p.client.PartitionService.GetPartitionID(keyData)
		partitions[partitionID] = append(partitions[partitionID], pair)
	}
	return partitions, nil
}

func (p *proxy) invokeOnKey(request *protocol.ClientMessage, keyData *serialization.Data) (*protocol.ClientMessage, error) {
	return p.client.InvocationService.invokeOnKeyOwner(request, keyData).Result()
}

func (p *proxy) invokeOnRandomTarget(request *protocol.ClientMessage) (*protocol.ClientMessage, error) {
	return p.client.InvocationService.invokeOnRandomTarget(request).Result()
}

func (p *proxy) invokeOnPartition(request *protocol.ClientMessage, partitionID int32) (*protocol.ClientMessage, error) {
	return p.client.InvocationService.invokeOnPartitionOwner(request, partitionID).Result()
}

func (p *proxy) invokeOnAddress(request *protocol.ClientMessage, address *protocol.Address) (*protocol.ClientMessage, error) {
	return p.client.InvocationService.invokeOnTarget(request, address).Result()
}

func (p *proxy) toObject(data *serialization.Data) (interface{}, error) {
	return p.client.SerializationService.ToObject(data)
}

func (p *proxy) toData(object interface{}) (*serialization.Data, error) {
	return p.client.SerializationService.ToData(object)
}

func (p *proxy) decodeToObjectAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() *serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return p.toObject(decodeFunc(responseMessage)())
}

func (p *proxy) decodeToBoolAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInterfaceSliceAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() []*serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectCollection(decodeFunc(responseMessage)(), p.client.SerializationService)
}

func (p *proxy) decodeToPairSliceAndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() []*protocol.Pair) (response []core.IPair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return collection.DataToObjectPairCollection(decodeFunc(responseMessage)(), p.client.SerializationService)
}

func (p *proxy) decodeToInt32AndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInt64AndError(responseMessage *protocol.ClientMessage, inputError error,
	decodeFunc func(*protocol.ClientMessage) func() int64) (response int64, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

type partitionSpecificProxy struct {
	*proxy
	partitionID int32
}

func newPartitionSpecificProxy(client *HazelcastClient, serviceName *string, name *string) (*partitionSpecificProxy, error) {
	var err error
	parSpecProxy := &partitionSpecificProxy{proxy: &proxy{client, serviceName, name}}
	parSpecProxy.partitionID, err = parSpecProxy.client.PartitionService.GetPartitionIDWithKey(parSpecProxy.PartitionKey())
	return parSpecProxy, err

}

func (parSpecProxy *partitionSpecificProxy) invoke(request *protocol.ClientMessage) (*protocol.ClientMessage, error) {
	return parSpecProxy.invokeOnPartition(request, parSpecProxy.partitionID)
}

func (p *proxy) createOnItemEvent(listener interface{}) func(itemData *serialization.Data, uuid *string, eventType int32) {
	return func(itemData *serialization.Data, uuid *string, eventType int32) {
		var item interface{}
		item, _ = p.toObject(itemData)
		member := p.client.ClusterService.MemberByUUID(*uuid)
		itemEvent := protocol.NewItemEvent(p.name, item, eventType, member.(*protocol.Member))
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
