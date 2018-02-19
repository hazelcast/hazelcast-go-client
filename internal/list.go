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
	"errors"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/common/collection"
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
	if element == nil {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := list.ToData(element)
	if err != nil {
		return false, err
	}
	request := ListAddEncodeRequest(list.name, valueData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListAddDecodeResponse(responseMessage).Response
	return changed, nil
}

func (list *ListProxy) AddAt(index int32, element interface{}) (err error) {
	if element == nil {
		return errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := list.ToData(element)
	if err != nil {
		return err
	}
	request := ListAddWithIndexEncodeRequest(list.name, index, valueData)
	_, err = list.Invoke(request)
	return err
}

func (list *ListProxy) AddAll(elements []interface{}) (changed bool, err error) {
	if len(elements) == 0 {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	itemsData := make([]*Data, len(elements))
	for index, item := range elements {
		itemData, err := list.ToData(item)
		if err != nil {
			return false, err
		}
		itemsData[index] = itemData
	}
	request := ListAddAllEncodeRequest(list.name, itemsData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListAddAllDecodeResponse(responseMessage).Response
	return changed, err

}

func (list *ListProxy) AddAllAt(index int32, elements []interface{}) (changed bool, err error) {
	if len(elements) == 0 {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementsData := make([]*Data, len(elements))
	for index, element := range elements {
		itemData, err := list.ToData(element)
		if err != nil {
			return false, err
		}
		elementsData[index] = itemData
	}
	request := ListAddAllWithIndexEncodeRequest(list.name, index, elementsData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListAddAllWithIndexDecodeResponse(responseMessage).Response
	return changed, err
}

func (list *ListProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := ListAddListenerEncodeRequest(list.name, includeValue, false)
	onItemEvent := func(itemData *Data, uuid *string, eventType int32) {
		var item interface{}
		if includeValue {
			item, _ = list.ToObject(itemData)
		}
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
	eventHandler := func(clientMessage *ClientMessage) {
		ListAddListenerHandle(clientMessage, func(itemData *Data, uuid *string, eventType int32) {
			onItemEvent(itemData, uuid, eventType)
		})
	}
	return list.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return ListRemoveListenerEncodeRequest(list.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return ListAddListenerDecodeResponse(clientMessage).Response
		})
}

func (list *ListProxy) Clear() (err error) {
	request := ListClearEncodeRequest(list.name)
	_, err = list.Invoke(request)
	return err
}

func (list *ListProxy) Contains(element interface{}) (found bool, err error) {
	if element == nil {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := list.ToData(element)
	if err != nil {
		return false, err
	}
	request := ListContainsEncodeRequest(list.name, valueData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	found = ListContainsDecodeResponse(responseMessage).Response
	return found, nil
}

func (list *ListProxy) ContainsAll(elements []interface{}) (foundAll bool, err error) {
	if len(elements) == 0 {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementsData, err := collection.ObjectToDataCollection(elements, list.client.SerializationService)
	if err != nil {
		return false, err
	}
	request := ListContainsAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, nil
	}
	foundAll = ListContainsAllDecodeResponse(responseMessage).Response
	return foundAll, nil
}

func (list *ListProxy) Get(index int32) (element interface{}, err error) {
	request := ListGetEncodeRequest(list.name, index)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return nil, err
	}
	element = ListGetDecodeResponse(responseMessage).Response
	return element, nil
}

func (list *ListProxy) IndexOf(element interface{}) (index int32, err error) {
	if element == nil {
		return 0, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementData, err := list.ToData(element)
	if err != nil {
		return 0, err
	}
	request := ListIndexOfEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return 0, err
	}
	index = ListIndexOfDecodeResponse(responseMessage).Response
	return index, nil
}

func (list *ListProxy) IsEmpty() (empty bool, err error) {
	request := ListIsEmptyEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	empty = ListIsEmptyDecodeResponse(responseMessage).Response
	return empty, nil
}

func (list *ListProxy) LastIndexOf(element interface{}) (index int32, err error) {
	if element == nil {
		return 0, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementData, err := list.ToData(element)
	if err != nil {
		return 0, err
	}
	request := ListLastIndexOfEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return 0, err
	}
	index = ListLastIndexOfDecodeResponse(responseMessage).Response
	return index, nil
}

func (list *ListProxy) Remove(element interface{}) (changed bool, err error) {
	if element == nil {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementData, err := list.ToData(element)
	if err != nil {
		return false, err
	}
	request := ListRemoveEncodeRequest(list.name, elementData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListRemoveDecodeResponse(responseMessage).Response
	return changed, nil
}

func (list *ListProxy) RemoveAt(index int32) (previousElement interface{}, err error) {
	request := ListRemoveWithIndexEncodeRequest(list.name, index)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return nil, err
	}
	previousElement = ListRemoveWithIndexDecodeResponse(responseMessage).Response
	return previousElement, nil
}

func (list *ListProxy) RemoveAll(elements []interface{}) (changed bool, err error) {
	if len(elements) == 0 {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementsData, err := collection.ObjectToDataCollection(elements, list.client.SerializationService)
	if err != nil {
		return false, err
	}
	request := ListCompareAndRemoveAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListCompareAndRemoveAllDecodeResponse(responseMessage).Response
	return changed, err
}

func (list *ListProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return list.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *ClientMessage {
		return ListRemoveListenerEncodeRequest(list.name, registrationID)
	})
}

func (list *ListProxy) RetainAll(elements []interface{}) (changed bool, err error) {
	if len(elements) == 0 {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	elementsData, err := collection.ObjectToDataCollection(elements, list.client.SerializationService)
	if err != nil {
		return false, err
	}
	request := ListCompareAndRetainAllEncodeRequest(list.name, elementsData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = ListCompareAndRetainAllDecodeResponse(responseMessage).Response
	return changed, err
}

func (list *ListProxy) Set(index int32, element interface{}) (previousElement interface{}, err error) {
	if element == nil {
		return nil, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := list.ToData(element)
	if err != nil {
		return nil, err
	}
	request := ListSetEncodeRequest(list.name, index, valueData)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return nil, err
	}
	previousElement = ListSetDecodeResponse(responseMessage).Response
	return previousElement, nil
}

func (list *ListProxy) Size() (size int32, err error) {
	request := ListSizeEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return 0, err
	}
	size = ListSizeDecodeResponse(responseMessage).Response
	return size, nil

}

func (list *ListProxy) SubList(start int32, end int32) (elements []interface{}, err error) {
	request := ListSubEncodeRequest(list.name, start, end)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return nil, err
	}
	elementsData := ListSubDecodeResponse(responseMessage).Response
	return collection.DataToObjectCollection(elementsData, list.client.SerializationService)

}

func (list *ListProxy) ToSlice() (elements []interface{}, err error) {
	request := ListGetAllEncodeRequest(list.name)
	responseMessage, err := list.Invoke(request)
	if err != nil {
		return nil, err
	}
	elementsData := ListGetAllDecodeResponse(responseMessage).Response
	return collection.DataToObjectCollection(elementsData, list.client.SerializationService)

}
