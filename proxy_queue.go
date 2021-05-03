/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Queue struct {
	*proxy
	partitionID int32
}

func newQueue(p *proxy) (*Queue, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &Queue{proxy: p, partitionID: partitionID}, nil
	}
}

func (q *Queue) Add(value interface{}) (bool, error) {
	return q.add(context.Background(), value, 0)
}

func (q *Queue) AddWithTimeout(value interface{}, timeout time.Duration) (bool, error) {
	return q.add(context.Background(), value, timeout.Milliseconds())
}

func (q *Queue) AddAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueAddAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueAddAllResponse(response), nil
		}
	}
}

func (q *Queue) AddListener(handler QueueItemNotifiedHandler) (internal.UUID, error) {
	return q.addListener(false, handler)
}

func (q *Queue) AddListenerIncludeValue(handler QueueItemNotifiedHandler) (internal.UUID, error) {
	return q.addListener(true, handler)
}

func (q *Queue) Clear() error {
	request := codec.EncodeQueueClearRequest(q.name)
	_, err := q.invokeOnPartition(context.Background(), request, q.partitionID)
	return err
}

func (q *Queue) Contains(value interface{}) (bool, error) {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsRequest(q.name, valueData)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsResponse(response), nil
		}
	}
}

func (q *Queue) ContainsAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsAllResponse(response), nil
		}
	}
}

func (q *Queue) Drain() ([]interface{}, error) {
	request := codec.EncodeQueueDrainToRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToResponse(response))
	}
}

func (q *Queue) DrainWithMaxSize(maxSize int) ([]interface{}, error) {
	request := codec.EncodeQueueDrainToMaxSizeRequest(q.name, int32(maxSize))
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToMaxSizeResponse(response))
	}
}

func (q *Queue) IsEmpty() (bool, error) {
	request := codec.EncodeQueueIsEmptyRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeQueueIsEmptyResponse(response), nil
	}
}

// TODO: iterator

func (q *Queue) Peek() (interface{}, error) {
	request := codec.EncodeQueuePeekRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueuePeekResponse(response))
	}
}

func (q *Queue) Poll() (interface{}, error) {
	return q.poll(context.Background(), 0)
}

func (q *Queue) PollWithTimeout(timeout time.Duration) (interface{}, error) {
	return q.poll(context.Background(), timeout.Milliseconds())
}

func (q *Queue) RemainingCapacity() (int, error) {
	request := codec.EncodeQueueRemainingCapacityRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueRemainingCapacityResponse(response)), nil
	}
}

func (q *Queue) Remove(value interface{}) (bool, error) {
	if data, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueRemoveRequest(q.name, data)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeQueueRemoveResponse(response), nil
		}
	}
}

func (q *Queue) RemoveAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRemoveAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRemoveAllResponse(response), nil
		}
	}
}

// RemoveListener removes the specified listener.
func (q *Queue) RemoveListener(subscriptionID internal.UUID) error {
	return q.listenerBinder.Remove(subscriptionID)
}

func (q *Queue) RetainAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRetainAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRetainAllResponse(response), nil
		}
	}
}

func (q *Queue) Size() (int, error) {
	request := codec.EncodeQueueSizeRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueSizeResponse(response)), nil
	}
}

func (q *Queue) Take() (interface{}, error) {
	request := codec.EncodeQueueTakeRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueueTakeResponse(response))
	}
}

func (q *Queue) add(ctx context.Context, value interface{}, timeout int64) (bool, error) {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return false, nil
	} else {
		request := codec.EncodeQueueOfferRequest(q.name, valueData, timeout)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeQueueOfferResponse(response), nil
		}
	}
}

func (q *Queue) addListener(includeValue bool, handler QueueItemNotifiedHandler) (internal.UUID, error) {
	subscriptionID := internal.NewUUID()
	addRequest := codec.EncodeQueueAddListenerRequest(q.name, includeValue, q.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeQueueRemoveListenerRequest(q.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleQueueAddListener(msg, func(itemData serialization.Data, uuid internal.UUID, eventType int32) {
			if item, err := q.convertToObject(itemData); err != nil {
				q.logger.Warnf("cannot convert data to Go value")
			} else {
				// TODO: get member from uuid
				handler(newQueueItemNotified(q.name, item, nil, eventType))
			}
		})
	}
	err := q.listenerBinder.Add(subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (q *Queue) poll(ctx context.Context, timeout int64) (interface{}, error) {
	request := codec.EncodeQueuePollRequest(q.name, timeout)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueuePollResponse(response))
	}
}

func (q *Queue) convertToObjects(data []serialization.Data) ([]interface{}, error) {
	decodedValues := []interface{}{}
	for _, datum := range data {
		if obj, err := q.convertToObject(datum); err != nil {
			return nil, err
		} else {
			decodedValues = append(decodedValues, obj)
		}
	}
	return decodedValues, nil
}
