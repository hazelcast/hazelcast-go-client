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
	request := QueueAddAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueAddAllDecodeResponse)
}

func (queue *QueueProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := QueueAddListenerEncodeRequest(queue.name, includeValue, false)
	eventHandler := queue.createEventHandler(listener)
	return queue.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return QueueRemoveListenerEncodeRequest(queue.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return QueueAddListenerDecodeResponse(clientMessage)()
		})
}

func (queue *QueueProxy) Clear() (err error) {
	request := QueueClearEncodeRequest(queue.name)
	_, err = queue.invoke(request)
	return err
}

func (queue *QueueProxy) Contains(item interface{}) (found bool, err error) {
	elementData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := QueueContainsEncodeRequest(queue.name, elementData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueContainsDecodeResponse)
}

func (queue *QueueProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := QueueContainsAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueContainsAllDecodeResponse)
}

func (queue *QueueProxy) DrainTo(slice *[]interface{}) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(NilSliceIsNotAllowed, nil)
	}
	request := QueueDrainToEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	resultSlice, err := queue.decodeToInterfaceSliceAndError(responseMessage, err, QueueDrainToDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (queue *QueueProxy) DrainToWithMaxSize(slice *[]interface{}, maxElements int32) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(NilSliceIsNotAllowed, nil)
	}
	request := QueueDrainToMaxSizeEncodeRequest(queue.name, maxElements)
	responseMessage, err := queue.invoke(request)
	resultSlice, err := queue.decodeToInterfaceSliceAndError(responseMessage, err, QueueDrainToMaxSizeDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (queue *QueueProxy) IsEmpty() (empty bool, err error) {
	request := QueueIsEmptyEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueIsEmptyDecodeResponse)
}

func (queue *QueueProxy) Offer(item interface{}) (added bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := QueueOfferEncodeRequest(queue.name, itemData, 0)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueOfferDecodeResponse)
}

func (queue *QueueProxy) OfferWithTimeout(item interface{}, timeout int64, timeoutUnit time.Duration) (added bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	timeoutInMilliSeconds := GetTimeInMilliSeconds(timeout, timeoutUnit)
	request := QueueOfferEncodeRequest(queue.name, itemData, timeoutInMilliSeconds)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueOfferDecodeResponse)
}

func (queue *QueueProxy) Peek() (item interface{}, err error) {
	request := QueuePeekEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, QueuePeekDecodeResponse)
}

func (queue *QueueProxy) Poll() (item interface{}, err error) {
	request := QueuePollEncodeRequest(queue.name, 0)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, QueuePollDecodeResponse)
}

func (queue *QueueProxy) PollWithTimeout(timeout int64, timeoutUnit time.Duration) (item interface{}, err error) {
	timeoutInMilliSeconds := GetTimeInMilliSeconds(timeout, timeoutUnit)
	request := QueuePollEncodeRequest(queue.name, timeoutInMilliSeconds)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, QueuePollDecodeResponse)
}

func (queue *QueueProxy) Put(item interface{}) (err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return err
	}
	request := QueuePutEncodeRequest(queue.name, itemData)
	_, err = queue.invoke(request)
	return err
}

func (queue *QueueProxy) RemainingCapacity() (remainingCapacity int32, err error) {
	request := QueueRemainingCapacityEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInt32AndError(responseMessage, err, QueueRemainingCapacityDecodeResponse)
}

func (queue *QueueProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := queue.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := QueueRemoveEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueRemoveDecodeResponse)
}

func (queue *QueueProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := QueueCompareAndRemoveAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueCompareAndRemoveAllDecodeResponse)
}

func (queue *QueueProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return queue.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *ClientMessage {
		return QueueRemoveListenerEncodeRequest(queue.name, registrationID)
	})
}

func (queue *QueueProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemData, err := queue.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := QueueCompareAndRetainAllEncodeRequest(queue.name, itemData)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToBoolAndError(responseMessage, err, QueueCompareAndRetainAllDecodeResponse)
}

func (queue *QueueProxy) Size() (size int32, err error) {
	request := QueueSizeEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInt32AndError(responseMessage, err, QueueSizeDecodeResponse)
}

func (queue *QueueProxy) Take() (item interface{}, err error) {
	request := QueueTakeEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToObjectAndError(responseMessage, err, QueueTakeDecodeResponse)
}

func (queue *QueueProxy) ToSlice() (items []interface{}, err error) {
	request := QueueIteratorEncodeRequest(queue.name)
	responseMessage, err := queue.invoke(request)
	return queue.decodeToInterfaceSliceAndError(responseMessage, err, QueueIteratorDecodeResponse)
}

func (queue *QueueProxy) createEventHandler(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		QueueAddListenerHandle(clientMessage, func(itemData *Data, uuid *string, eventType int32) {
			onItemEvent := queue.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
