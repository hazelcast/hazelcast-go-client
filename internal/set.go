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

type setProxy struct {
	*partitionSpecificProxy
}

func newSetProxy(client *HazelcastClient, serviceName *string, name *string) (*setProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &setProxy{parSpecProxy}, nil
}

func (sp *setProxy) Add(item interface{}) (added bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.SetAddEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetAddDecodeResponse)
}

func (sp *setProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetAddAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetAddAllDecodeResponse)
}

func (sp *setProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.SetAddListenerEncodeRequest(sp.name, includeValue, false)
	eventHandler := sp.createEventHandler(listener)
	return sp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID *string) *protocol.ClientMessage {
			return protocol.SetRemoveListenerEncodeRequest(sp.name, registrationID)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.SetAddListenerDecodeResponse(clientMessage)()
		})

}

func (sp *setProxy) Clear() (err error) {
	request := protocol.SetClearEncodeRequest(sp.name)
	_, err = sp.invoke(request)
	return err
}

func (sp *setProxy) Contains(item interface{}) (found bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.SetContainsEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetContainsDecodeResponse)
}

func (sp *setProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetContainsAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetContainsAllDecodeResponse)
}

func (sp *setProxy) IsEmpty() (empty bool, err error) {
	request := protocol.SetIsEmptyEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetIsEmptyDecodeResponse)
}

func (sp *setProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.SetRemoveEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetRemoveDecodeResponse)
}

func (sp *setProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetCompareAndRemoveAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetCompareAndRemoveAllDecodeResponse)
}

func (sp *setProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.SetCompareAndRetainAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, protocol.SetCompareAndRetainAllDecodeResponse)
}

func (sp *setProxy) Size() (size int32, err error) {
	request := protocol.SetSizeEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToInt32AndError(responseMessage, err, protocol.SetSizeDecodeResponse)
}

func (sp *setProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return sp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.SetRemoveListenerEncodeRequest(sp.name, registrationID)
	})
}

func (sp *setProxy) ToSlice() (items []interface{}, err error) {
	request := protocol.SetGetAllEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.SetGetAllDecodeResponse)
}

func (sp *setProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.SetAddListenerHandle(clientMessage, func(itemData *serialization.Data, uuid *string, eventType int32) {
			onItemEvent := sp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
