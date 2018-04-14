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
	request := protocol.SetAddEncodeRequest(set.name, itemData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetAddDecodeResponse)
}

func (set *SetProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetAddAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetAddAllDecodeResponse)
}

func (set *SetProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.SetAddListenerEncodeRequest(set.name, includeValue, false)
	eventHandler := set.createEventHandler(listener)
	return set.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.SetRemoveListenerEncodeRequest(set.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.SetAddListenerDecodeResponse(clientMessage)()
		})

}

func (set *SetProxy) Clear() (err error) {
	request := protocol.SetClearEncodeRequest(set.name)
	_, err = set.invoke(request)
	return err
}

func (set *SetProxy) Contains(item interface{}) (found bool, err error) {
	itemData, err := set.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.SetContainsEncodeRequest(set.name, itemData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetContainsDecodeResponse)
}

func (set *SetProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetContainsAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetContainsAllDecodeResponse)
}

func (set *SetProxy) IsEmpty() (empty bool, err error) {
	request := protocol.SetIsEmptyEncodeRequest(set.name)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetIsEmptyDecodeResponse)
}

func (set *SetProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := set.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.SetRemoveEncodeRequest(set.name, itemData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetRemoveDecodeResponse)
}

func (set *SetProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetCompareAndRemoveAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetCompareAndRemoveAllDecodeResponse)
}

func (set *SetProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemsData, err := set.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetCompareAndRetainAllEncodeRequest(set.name, itemsData)
	responseMessage, err := set.invoke(request)
	return set.decodeToBoolAndError(responseMessage, err, protocol.SetCompareAndRetainAllDecodeResponse)
}

func (set *SetProxy) Size() (size int32, err error) {
	request := protocol.SetSizeEncodeRequest(set.name)
	responseMessage, err := set.invoke(request)
	return set.decodeToInt32AndError(responseMessage, err, protocol.SetSizeDecodeResponse)
}

func (set *SetProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return set.client.ListenerService.deregisterListener(*registrationID, func(registrationId *string) *protocol.ClientMessage {
		return protocol.SetRemoveListenerEncodeRequest(set.name, registrationId)
	})
}

func (set *SetProxy) ToSlice() (items []interface{}, err error) {
	request := protocol.SetGetAllEncodeRequest(set.name)
	responseMessage, err := set.invoke(request)
	return set.decodeToInterfaceSliceAndError(responseMessage, err, protocol.SetGetAllDecodeResponse)
}

func (set *SetProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.SetAddListenerHandle(clientMessage, func(itemData *serialization.Data, uuid *string, eventType int32) {
			onItemEvent := set.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
