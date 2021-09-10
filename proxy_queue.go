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

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	iserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
Queue is a concurrent, blocking, distributed, observable queue.

Queue is not a partitioned data-structure.
All of the Queue content is stored in a single machine (and in the backup).
Queue will not scale by adding more members in the cluster.

For details see https://docs.hazelcast.com/imdg/latest/data-structures/queue.html
*/
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

// Add adds the specified item to this queue if there is available space.
// Returns true when element is successfully added
func (q *Queue) Add(ctx context.Context, value interface{}) (bool, error) {
	return q.add(ctx, value, 0)
}

// AddWithTimeout adds the specified item to this queue if there is available space.
// Returns true when element is successfully added
func (q *Queue) AddWithTimeout(ctx context.Context, value interface{}, timeout time.Duration) (bool, error) {
	return q.add(ctx, value, timeout.Milliseconds())
}

// AddAll adds the elements in the specified collection to this queue.
// Returns true if the queue is changed after the call.
func (q *Queue) AddAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := q.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueAddAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueAddAllResponse(response), nil
		}
	}
}

// AddItemListener adds an item listener for this queue. Listener will be notified for all queue add/remove events.
// Received events include the updated item if includeValue is true.
func (q *Queue) AddItemListener(ctx context.Context, includeValue bool, handler QueueItemNotifiedHandler) (types.UUID, error) {
	return q.addListener(ctx, includeValue, handler)
}

// Clear Clear this queue. Queue will be empty after this call.
func (q *Queue) Clear(ctx context.Context) error {
	request := codec.EncodeQueueClearRequest(q.name)
	_, err := q.invokeOnPartition(ctx, request, q.partitionID)
	return err
}

// Contains returns true if the queue includes the given value.
func (q *Queue) Contains(ctx context.Context, value interface{}) (bool, error) {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsRequest(q.name, valueData)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsResponse(response), nil
		}
	}
}

// ContainsAll returns true if the queue includes all given values.
func (q *Queue) ContainsAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := q.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsAllResponse(response), nil
		}
	}
}

// Drain returns all items in the queue and empties it.
func (q *Queue) Drain(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeQueueDrainToRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToResponse(response))
	}
}

// DrainWithMaxSize returns maximum maxSize items in tne queue and removes returned items from the queue.
func (q *Queue) DrainWithMaxSize(ctx context.Context, maxSize int) ([]interface{}, error) {
	maxSizeAsInt32, err := validationutil.ValidateAsNonNegativeInt32(maxSize)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeQueueDrainToMaxSizeRequest(q.name, maxSizeAsInt32)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToMaxSizeResponse(response))
	}
}

// GetAll returns all of the items in this queue.
func (q *Queue) GetAll(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeQueueIteratorRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueIteratorResponse(response))
	}
}

// IsEmpty returns true if the queue is empty.
func (q *Queue) IsEmpty(ctx context.Context) (bool, error) {
	request := codec.EncodeQueueIsEmptyRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeQueueIsEmptyResponse(response), nil
	}
}

// Peek retrieves the head of queue without removing it from the queue.
func (q *Queue) Peek(ctx context.Context) (interface{}, error) {
	request := codec.EncodeQueuePeekRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueuePeekResponse(response))
	}
}

// Poll retrieves and removes the head of this queue.
func (q *Queue) Poll(ctx context.Context) (interface{}, error) {
	return q.poll(ctx, 0)
}

// PollWithTimeout retrieves and removes the head of this queue.
// Waits until this timeout elapses and returns the result.
func (q *Queue) PollWithTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	return q.poll(ctx, timeout.Milliseconds())
}

// Put adds the specified element into this queue.
// If there is no space, it waits until necessary space becomes available.
func (q *Queue) Put(ctx context.Context, value interface{}) error {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return err
	} else {
		request := codec.EncodeQueuePutRequest(q.name, valueData)
		_, err := q.invokeOnPartition(ctx, request, q.partitionID)
		return err
	}
}

// RemainingCapacity returns the remaining capacity of this queue.
func (q *Queue) RemainingCapacity(ctx context.Context) (int, error) {
	request := codec.EncodeQueueRemainingCapacityRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueRemainingCapacityResponse(response)), nil
	}
}

// Remove removes the specified element from the queue if it exists.
func (q *Queue) Remove(ctx context.Context, value interface{}) (bool, error) {
	if data, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueRemoveRequest(q.name, data)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeQueueRemoveResponse(response), nil
		}
	}
}

// RemoveAll removes all of the elements of the specified collection from this queue.
func (q *Queue) RemoveAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := q.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRemoveAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRemoveAllResponse(response), nil
		}
	}
}

// RemoveListener removes the specified listener.
func (q *Queue) RemoveListener(ctx context.Context, subscriptionID types.UUID) error {
	return q.listenerBinder.Remove(ctx, subscriptionID)
}

// RetainAll removes the items which are not contained in the specified collection.
func (q *Queue) RetainAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := q.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRetainAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRetainAllResponse(response), nil
		}
	}
}

// Size returns the number of elements in this collection.
func (q *Queue) Size(ctx context.Context) (int, error) {
	request := codec.EncodeQueueSizeRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueSizeResponse(response)), nil
	}
}

// Take retrieves and removes the head of this queue, if necessary, waits until an item becomes available.
func (q *Queue) Take(ctx context.Context) (interface{}, error) {
	request := codec.EncodeQueueTakeRequest(q.name)
	if response, err := q.invokeOnPartition(ctx, request, q.partitionID); err != nil {
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
			return false, err
		} else {
			return codec.DecodeQueueOfferResponse(response), nil
		}
	}
}

func (q *Queue) addListener(ctx context.Context, includeValue bool, handler QueueItemNotifiedHandler) (types.UUID, error) {
	subscriptionID := types.NewUUID()
	addRequest := codec.EncodeQueueAddListenerRequest(q.name, includeValue, q.smart)
	removeRequest := codec.EncodeQueueRemoveListenerRequest(q.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleQueueAddListener(msg, func(itemData iserialization.Data, uuid types.UUID, eventType int32) {
			if item, err := q.convertToObject(itemData); err != nil {
				q.logger.Warnf("cannot convert data to Go value")
			} else {
				member := q.clusterService.GetMemberByUUID(uuid)
				handler(newQueueItemNotified(q.name, item, *member, eventType))
			}
		})
	}
	err := q.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
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
