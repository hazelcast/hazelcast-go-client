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
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type queueProxy struct {
	*partitionSpecificProxy
}

func newQueueProxy(client *HazelcastClient, serviceName *string, name *string) (*queueProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &queueProxy{parSpecProxy}, nil
}

func (qp *queueProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueAddAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueAddAllDecodeResponse)
}

func (qp *queueProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.QueueAddListenerEncodeRequest(qp.name, includeValue, false)
	eventHandler := qp.createEventHandler(listener)
	return qp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID *string) *protocol.ClientMessage {
			return protocol.QueueRemoveListenerEncodeRequest(qp.name, registrationID)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.QueueAddListenerDecodeResponse(clientMessage)()
		})
}

func (qp *queueProxy) Clear() (err error) {
	request := protocol.QueueClearEncodeRequest(qp.name)
	_, err = qp.invoke(request)
	return err
}

func (qp *queueProxy) Contains(item interface{}) (found bool, err error) {
	elementData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueContainsEncodeRequest(qp.name, elementData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueContainsDecodeResponse)
}

func (qp *queueProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueContainsAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueContainsAllDecodeResponse)
}

func (qp *queueProxy) DrainTo(slice *[]interface{}) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	request := protocol.QueueDrainToEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	resultSlice, err := qp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueDrainToDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (qp *queueProxy) DrainToWithMaxSize(slice *[]interface{}, maxElements int32) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	request := protocol.QueueDrainToMaxSizeEncodeRequest(qp.name, maxElements)
	responseMessage, err := qp.invoke(request)
	resultSlice, err := qp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueDrainToMaxSizeDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (qp *queueProxy) IsEmpty() (empty bool, err error) {
	request := protocol.QueueIsEmptyEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueIsEmptyDecodeResponse)
}

func (qp *queueProxy) Offer(item interface{}) (added bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueOfferEncodeRequest(qp.name, itemData, 0)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueOfferDecodeResponse)
}

func (qp *queueProxy) OfferWithTimeout(item interface{}, timeout time.Duration) (added bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	timeoutInMilliSeconds := bufutil.GetTimeInMilliSeconds(timeout)
	request := protocol.QueueOfferEncodeRequest(qp.name, itemData, timeoutInMilliSeconds)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueOfferDecodeResponse)
}

func (qp *queueProxy) Peek() (item interface{}, err error) {
	request := protocol.QueuePeekEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, protocol.QueuePeekDecodeResponse)
}

func (qp *queueProxy) Poll() (item interface{}, err error) {
	request := protocol.QueuePollEncodeRequest(qp.name, 0)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, protocol.QueuePollDecodeResponse)
}

func (qp *queueProxy) PollWithTimeout(timeout time.Duration) (item interface{}, err error) {
	timeoutInMilliSeconds := bufutil.GetTimeInMilliSeconds(timeout)
	request := protocol.QueuePollEncodeRequest(qp.name, timeoutInMilliSeconds)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, protocol.QueuePollDecodeResponse)
}

func (qp *queueProxy) Put(item interface{}) (err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return err
	}
	request := protocol.QueuePutEncodeRequest(qp.name, itemData)
	_, err = qp.invoke(request)
	return err
}

func (qp *queueProxy) RemainingCapacity() (remainingCapacity int32, err error) {
	request := protocol.QueueRemainingCapacityEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInt32AndError(responseMessage, err, protocol.QueueRemainingCapacityDecodeResponse)
}

func (qp *queueProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := protocol.QueueRemoveEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueRemoveDecodeResponse)
}

func (qp *queueProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueCompareAndRemoveAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueCompareAndRemoveAllDecodeResponse)
}

func (qp *queueProxy) RemoveItemListener(registrationID *string) (removed bool, err error) {
	return qp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.QueueRemoveListenerEncodeRequest(qp.name, registrationID)
	})
}

func (qp *queueProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := protocol.QueueCompareAndRetainAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, protocol.QueueCompareAndRetainAllDecodeResponse)
}

func (qp *queueProxy) Size() (size int32, err error) {
	request := protocol.QueueSizeEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInt32AndError(responseMessage, err, protocol.QueueSizeDecodeResponse)
}

func (qp *queueProxy) Take() (item interface{}, err error) {
	request := protocol.QueueTakeEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, protocol.QueueTakeDecodeResponse)
}

func (qp *queueProxy) ToSlice() (items []interface{}, err error) {
	request := protocol.QueueIteratorEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.QueueIteratorDecodeResponse)
}

func (qp *queueProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.QueueAddListenerHandle(clientMessage, func(itemData *serialization.Data, uuid *string, eventType int32) {
			onItemEvent := qp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
