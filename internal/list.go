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
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
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

func (list *ListProxy) Add(element interface{}) (changed bool, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := ListAddEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListAddDecodeResponse)
}

func (list *ListProxy) AddAt(index int32, element interface{}) (err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return err
	}
	request := ListAddWithIndexEncodeRequest(list.name, index, elementData)
	_, err = list.Invoke(request)
	return err
}

func (list *ListProxy) AddAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := list.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := ListAddAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListAddAllDecodeResponse)

}

func (list *ListProxy) AddAllAt(index int32, elements []interface{}) (changed bool, err error) {
	elementsData, err := list.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := ListAddAllWithIndexEncodeRequest(list.name, index, elementsData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListAddAllWithIndexDecodeResponse)
}

func (list *ListProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := ListAddListenerEncodeRequest(list.name, includeValue, false)
	eventHandler := list.createEventHandler(listener)
	return list.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return ListRemoveListenerEncodeRequest(list.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return ListAddListenerDecodeResponse(clientMessage)()
		})
}

func (list *ListProxy) Clear() (err error) {
	request := ListClearEncodeRequest(list.name)
	_, err = list.Invoke(request)
	return err
}

func (list *ListProxy) Contains(element interface{}) (found bool, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := ListContainsEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListContainsDecodeResponse)
}

func (list *ListProxy) ContainsAll(elements []interface{}) (foundAll bool, err error) {
	elementsData, err := list.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := ListContainsAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListContainsAllDecodeResponse)
}

func (list *ListProxy) Get(index int32) (element interface{}, err error) {
	request := ListGetEncodeRequest(list.name, index)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToObjectAndError(responseMessage, err, ListGetDecodeResponse)
}

func (list *ListProxy) IndexOf(element interface{}) (index int32, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := ListIndexOfEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToInt32AndError(responseMessage, err, ListIndexOfDecodeResponse)
}

func (list *ListProxy) IsEmpty() (empty bool, err error) {
	request := ListIsEmptyEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListIsEmptyDecodeResponse)
}

func (list *ListProxy) LastIndexOf(element interface{}) (index int32, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := ListLastIndexOfEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToInt32AndError(responseMessage, err, ListIndexOfDecodeResponse)
}

func (list *ListProxy) Remove(element interface{}) (changed bool, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := ListRemoveEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListRemoveDecodeResponse)
}

func (list *ListProxy) RemoveAt(index int32) (previousElement interface{}, err error) {
	request := ListRemoveWithIndexEncodeRequest(list.name, index)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToObjectAndError(responseMessage, err, ListRemoveWithIndexDecodeResponse)
}

func (list *ListProxy) RemoveAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := list.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := ListCompareAndRemoveAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListCompareAndRemoveAllDecodeResponse)
}

func (list *ListProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return list.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *ClientMessage {
		return ListRemoveListenerEncodeRequest(list.name, registrationID)
	})
}

func (list *ListProxy) RetainAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := list.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := ListCompareAndRetainAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToBoolAndError(responseMessage, err, ListCompareAndRetainAllDecodeResponse)
}

func (list *ListProxy) Set(index int32, element interface{}) (previousElement interface{}, err error) {
	elementData, err := list.validateAndSerialize(element)
	if err != nil {
		return nil, err
	}
	request := ListSetEncodeRequest(list.name, index, elementData)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToObjectAndError(responseMessage, err, ListSetDecodeResponse)
}

func (list *ListProxy) Size() (size int32, err error) {
	request := ListSizeEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToInt32AndError(responseMessage, err, ListSizeDecodeResponse)
}

func (list *ListProxy) SubList(start int32, end int32) (elements []interface{}, err error) {
	request := ListSubEncodeRequest(list.name, start, end)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToInterfaceSliceAndError(responseMessage, err, ListSubDecodeResponse)
}

func (list *ListProxy) ToSlice() (elements []interface{}, err error) {
	request := ListGetAllEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	return list.DecodeToInterfaceSliceAndError(responseMessage, err, ListGetAllDecodeResponse)
}

func (list *ListProxy) createEventHandler(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		ListAddListenerHandle(clientMessage, func(itemData *Data, uuid *string, eventType int32) {
			onItemEvent := list.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}

func (list *ListProxy) createOnItemEvent(listener interface{}) func(itemData *Data, uuid *string, eventType int32) {
	return func(itemData *Data, uuid *string, eventType int32) {
		var item interface{}
		item, _ = list.ToObject(itemData)
		member := list.client.ClusterService.GetMemberByUuid(*uuid)
		itemEvent := NewItemEvent(list.name, item, eventType, member.(*Member))
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
