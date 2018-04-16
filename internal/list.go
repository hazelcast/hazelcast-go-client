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
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type ListProxy struct {
	*partitionSpecificProxy
}

func newListProxy(client *HazelcastClient, serviceName *string, name *string) (*ListProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &ListProxy{parSpecProxy}, nil
}

func (lp *ListProxy) Add(element interface{}) (changed bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := protocol.ListAddEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListAddDecodeResponse)
}

func (lp *ListProxy) AddAt(index int32, element interface{}) (err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return err
	}
	request := protocol.ListAddWithIndexEncodeRequest(lp.name, index, elementData)
	_, err = lp.invoke(request)
	return err
}

func (lp *ListProxy) AddAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := protocol.ListAddAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListAddAllDecodeResponse)

}

func (lp *ListProxy) AddAllAt(index int32, elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := protocol.ListAddAllWithIndexEncodeRequest(lp.name, index, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListAddAllWithIndexDecodeResponse)
}

func (lp *ListProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.ListAddListenerEncodeRequest(lp.name, includeValue, false)
	eventHandler := lp.createEventHandler(listener)
	return lp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.ListRemoveListenerEncodeRequest(lp.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.ListAddListenerDecodeResponse(clientMessage)()
		})
}

func (lp *ListProxy) Clear() (err error) {
	request := protocol.ListClearEncodeRequest(lp.name)
	_, err = lp.invoke(request)
	return err
}

func (lp *ListProxy) Contains(element interface{}) (found bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := protocol.ListContainsEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListContainsDecodeResponse)
}

func (lp *ListProxy) ContainsAll(elements []interface{}) (foundAll bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := protocol.ListContainsAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListContainsAllDecodeResponse)
}

func (lp *ListProxy) Get(index int32) (element interface{}, err error) {
	request := protocol.ListGetEncodeRequest(lp.name, index)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, protocol.ListGetDecodeResponse)
}

func (lp *ListProxy) IndexOf(element interface{}) (index int32, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := protocol.ListIndexOfEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, protocol.ListIndexOfDecodeResponse)
}

func (lp *ListProxy) IsEmpty() (empty bool, err error) {
	request := protocol.ListIsEmptyEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListIsEmptyDecodeResponse)
}

func (lp *ListProxy) LastIndexOf(element interface{}) (index int32, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := protocol.ListLastIndexOfEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, protocol.ListIndexOfDecodeResponse)
}

func (lp *ListProxy) Remove(element interface{}) (changed bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := protocol.ListRemoveEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListRemoveDecodeResponse)
}

func (lp *ListProxy) RemoveAt(index int32) (previousElement interface{}, err error) {
	request := protocol.ListRemoveWithIndexEncodeRequest(lp.name, index)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, protocol.ListRemoveWithIndexDecodeResponse)
}

func (lp *ListProxy) RemoveAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := protocol.ListCompareAndRemoveAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListCompareAndRemoveAllDecodeResponse)
}

func (lp *ListProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return lp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ListRemoveListenerEncodeRequest(lp.name, registrationID)
	})
}

func (lp *ListProxy) RetainAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := protocol.ListCompareAndRetainAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, protocol.ListCompareAndRetainAllDecodeResponse)
}

func (lp *ListProxy) Set(index int32, element interface{}) (previousElement interface{}, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return nil, err
	}
	request := protocol.ListSetEncodeRequest(lp.name, index, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, protocol.ListSetDecodeResponse)
}

func (lp *ListProxy) Size() (size int32, err error) {
	request := protocol.ListSizeEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, protocol.ListSizeDecodeResponse)
}

func (lp *ListProxy) SubList(start int32, end int32) (elements []interface{}, err error) {
	request := protocol.ListSubEncodeRequest(lp.name, start, end)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.ListSubDecodeResponse)
}

func (lp *ListProxy) ToSlice() (elements []interface{}, err error) {
	request := protocol.ListGetAllEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.ListGetAllDecodeResponse)
}

func (lp *ListProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.ListAddListenerHandle(clientMessage, func(itemData *serialization.Data, uuid *string, eventType int32) {
			onItemEvent := lp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
