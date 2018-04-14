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
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"time"
)

type QueueProxy struct {
	*partitionSpecificProxy
}

func newQueueProxy(client *HazelcastClient, serviceName *string, name *string) (*QueueProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &QueueProxy{parSpecProxy}, nil
}

func (queue *QueueProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueAddAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueAddAllDecodeResponse)
}

func (queue *QueueProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.QueueAddListenerEncodeRequest(queue.name, includeValue, false)
	eventHandler := queue.createEventHandler(listener)
	return queue.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.QueueRemoveListenerEncodeRequest(queue.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.QueueAddListenerDecodeResponse(clientMessage)()
		})
}

func (queue *QueueProxy) Clear() (err error) {
	request := protocol.QueueClearEncodeRequest(queue.name)
	_, err = queue.invoke(request)
	return err
}

func (queue *QueueProxy) Contains(item interface{}) (found bool, err error) {
	elementData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueContainsEncodeRequest(queue.name, elementData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueContainsDecodeResponse)
}

func (queue *QueueProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueContainsAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueContainsAllDecodeResponse)
}

func (queue *QueueProxy) DrainTo(slice *[]interface{}) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(common.NilSliceIsNotAllowed, nil)
	}
	request := protocol.QueueDrainToEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	resultSlice, err := queue.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueDrainToDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (queue *QueueProxy) DrainToWithMaxSize(slice *[]interface{}, maxElements int32) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(common.NilSliceIsNotAllowed, nil)
	}
	request := protocol.QueueDrainToMaxSizeEncodeRequest(queue.name, maxElements)
	responseMessage, err := queue.invoke(request)
	resultSlice, err := queue.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueDrainToMaxSizeDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (queue *QueueProxy) IsEmpty() (empty bool, err error) {
	request := protocol.QueueIsEmptyEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueIsEmptyDecodeResponse)
}

func (queue *QueueProxy) Offer(item interface{}) (added bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueOfferEncodeRequest(queue.name, itemData, 0)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueOfferDecodeResponse)
}

func (queue *QueueProxy) OfferWithTimeout(item interface{}, timeout int64, timeoutUnit time.Duration) (added bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	timeoutInMilliSeconds := common.GetTimeInMilliSeconds(timeout, timeoutUnit)
	request := protocol.QueueOfferEncodeRequest(queue.name, itemData, timeoutInMilliSeconds)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueOfferDecodeResponse)
}

func (queue *QueueProxy) Peek() (item interface{}, err error) {
	request := protocol.QueuePeekEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, protocol.QueuePeekDecodeResponse)
}

func (queue *QueueProxy) Poll() (item interface{}, err error) {
	request := protocol.QueuePollEncodeRequest(queue.name, 0)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, protocol.QueuePollDecodeResponse)
}

func (queue *QueueProxy) PollWithTimeout(timeout int64, timeoutUnit time.Duration) (item interface{}, err error) {
	timeoutInMilliSeconds := common.GetTimeInMilliSeconds(timeout, timeoutUnit)
	request := protocol.QueuePollEncodeRequest(queue.name, timeoutInMilliSeconds)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, protocol.QueuePollDecodeResponse)
}

func (queue *QueueProxy) Put(item interface{}) (err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return err
	}
	request := protocol.QueuePutEncodeRequest(queue.name, itemData)
	_, err = queue.invoke(request)
	return err
}

func (queue *QueueProxy) RemainingCapacity() (remainingCapacity int32, err error) {
	request := protocol.QueueRemainingCapacityEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInt32AndError(responseMessage, err, protocol.QueueRemainingCapacityDecodeResponse)
}

func (queue *QueueProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueRemoveEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueRemoveDecodeResponse)
}

func (queue *QueueProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueCompareAndRemoveAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueCompareAndRemoveAllDecodeResponse)
}

func (queue *QueueProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return queue.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.QueueRemoveListenerEncodeRequest(queue.name, registrationID)
	})
}

func (queue *QueueProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueCompareAndRetainAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, protocol.QueueCompareAndRetainAllDecodeResponse)
}

func (queue *QueueProxy) Size() (size int32, err error) {
	request := protocol.QueueSizeEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInt32AndError(responseMessage, err, protocol.QueueSizeDecodeResponse)
}

func (queue *QueueProxy) Take() (item interface{}, err error) {
	request := protocol.QueueTakeEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, protocol.QueueTakeDecodeResponse)
}

func (queue *QueueProxy) ToSlice() (items []interface{}, err error) {
	request := protocol.QueueIteratorEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueIteratorDecodeResponse)
}

func (queue *QueueProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.QueueAddListenerHandle(clientMessage, func(itemData *serialization.Data, uuid *string, eventType int32) {
			onItemEvent := queue.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
