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
	"github.com/hazelcast/hazelcast-go-client/internal/common/collection"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type SetProxy struct {
	*partitionSpecificProxy
}

func newSetProxy(client *HazelcastClient, serviceName *string, name *string) (*SetProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &SetProxy{parSpecProxy}, nil
}

func (set *SetProxy) Add(item interface{}) (added bool, err error) {
	itemData, err := set.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := SetAddEncodeRequest(set.name, itemData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	added = SetAddDecodeResponse(responseMessage).Response
	return added, nil
}

func (set *SetProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := SetAddAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = SetAddAllDecodeResponse(responseMessage).Response
	return changed, nil
}
func (set *SetProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	return nil, nil
	/*
		request := ListAddListenerEncodeRequest(set.name, includeValue, false)

		onItemEvent := func(itemData *Data, uuid *string, eventType int32) {
			var item interface{}
			if includeValue {
				item, _ = set.ToObject(itemData)
			}
			member := set.client.ClusterService.GetMemberByUuid(*uuid)
			itemEvent := NewItemEvent(set.name, item, eventType, member.(*Member))
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
			SetAddListenerHandle(clientMessage, func(itemData *Data, uuid *string, eventType int32) {
				onItemEvent(itemData, uuid, eventType)
			})
		}
		return set.client.ListenerService.registerListener(request, eventHandler,
			func(registrationId *string) *ClientMessage {
				return ListRemoveListenerEncodeRequest(set.name, registrationId)
			}, func(clientMessage *ClientMessage) *string {
				return ListAddListenerDecodeResponse(clientMessage).Response
			})
	*/

}

func (set *SetProxy) Clear() (err error) {
	request := SetClearEncodeRequest(set.name)
	_, err = set.Invoke(request)
	return err
}

func (set *SetProxy) Contains(item interface{}) (found bool, err error) {
	itemData, err := set.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := SetContainsEncodeRequest(set.name, itemData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	found = SetContainsDecodeResponse(responseMessage).Response
	return found, nil
}

func (set *SetProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := SetContainsAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	foundAll = SetContainsAllDecodeResponse(responseMessage).Response
	return foundAll, nil
}

func (set *SetProxy) IsEmpty() (empty bool, err error) {
	request := SetIsEmptyEncodeRequest(set.name)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	empty = SetIsEmptyDecodeResponse(responseMessage).Response
	return empty, nil
}

func (set *SetProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := set.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := SetRemoveEncodeRequest(set.name, itemData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	removed = SetRemoveDecodeResponse(responseMessage).Response
	return removed, nil
}

func (set *SetProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := SetCompareAndRemoveAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = SetCompareAndRemoveAllDecodeResponse(responseMessage).Response
	return changed, nil
}

func (set *SetProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := SetCompareAndRetainAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return false, err
	}
	changed = SetCompareAndRetainAllDecodeResponse(responseMessage).Response
	return changed, nil
}

func (set *SetProxy) Size() (size int32, err error) {
	request := SetSizeEncodeRequest(set.name)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return 0, err
	}
	size = SetSizeDecodeResponse(responseMessage).Response
	return size, nil
}

func (set *SetProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return set.client.ListenerService.deregisterListener(*registrationID, func(registrationId *string) *ClientMessage {
		return SetRemoveListenerEncodeRequest(set.name, registrationId)
	})
}

func (set *SetProxy) ToSlice() (items []interface{}, err error) {
	request := SetGetAllEncodeRequest(set.name)
	responseMessage, err := set.Invoke(request)
	if err != nil {
		return nil, err
	}
	itemsData := SetGetAllDecodeResponse(responseMessage).Response
	items, err = collection.DataToObjectCollection(itemsData, set.client.SerializationService)
	return items, nil
}
