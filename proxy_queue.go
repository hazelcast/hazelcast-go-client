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

	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

/*
Queue is a concurrent, blocking, distributed, observable queue.

Queue is not a partitioned data-structure.
All of the Queue content is stored in a single machine (and in the backup).
Queue will not scale by adding more members in the cluster.
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
func (q *Queue) Add(value interface{}) (bool, error) {
	return q.add(context.TODO(), value, 0)
}

// AddWithTimeout adds the specified item to this queue if there is available space.
// Returns true when element is successfully added
func (q *Queue) AddWithTimeout(value interface{}, timeout time.Duration) (bool, error) {
	return q.add(context.TODO(), value, timeout.Milliseconds())
}

// AddAll adds the elements in the specified collection to this queue.
// Returns true if the queue is changed after the call.
func (q *Queue) AddAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueAddAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueAddAllResponse(response), nil
		}
	}
}

// AddListener adds an item listener for this queue. Listener will be notified for all queue add/remove events.
func (q *Queue) AddListener(handler QueueItemNotifiedHandler) (types.UUID, error) {
	return q.addListener(false, handler)
}

// AddListenerIncludeValue adds an item listener for this queue. Listener will be notified for all queue add/remove events.
// Received events inclues the updated item.
func (q *Queue) AddListenerIncludeValue(handler QueueItemNotifiedHandler) (types.UUID, error) {
	return q.addListener(true, handler)
}

// Clear Clear this queue. Queue will be empty after this call.
func (q *Queue) Clear() error {
	request := codec.EncodeQueueClearRequest(q.name)
	_, err := q.invokeOnPartition(context.TODO(), request, q.partitionID)
	return err
}

// Contains returns true if the queue includes the given value.
func (q *Queue) Contains(value interface{}) (bool, error) {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsRequest(q.name, valueData)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsResponse(response), nil
		}
	}
}

// ContainsAll returns true if the queue includes all given values.
func (q *Queue) ContainsAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueContainsAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueContainsAllResponse(response), nil
		}
	}
}

// Drain returns all items in the queue and empties it.
func (q *Queue) Drain() ([]interface{}, error) {
	request := codec.EncodeQueueDrainToRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToResponse(response))
	}
}

// DrainWithMaxSize returns maximum maxSize items in tne queue and removes returned items from the queue.
func (q *Queue) DrainWithMaxSize(maxSize int) ([]interface{}, error) {
	request := codec.EncodeQueueDrainToMaxSizeRequest(q.name, int32(maxSize))
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueDrainToMaxSizeResponse(response))
	}

}

// Iterator returns all of the items in this queue.
func (q *Queue) Iterator() ([]interface{}, error) {
	request := codec.EncodeQueueIteratorRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObjects(codec.DecodeQueueIteratorResponse(response))
	}
}

// IsEmpty returns true if the queue is empty.
func (q *Queue) IsEmpty() (bool, error) {
	request := codec.EncodeQueueIsEmptyRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeQueueIsEmptyResponse(response), nil
	}
}

// Peek retrieves the head of queue without removing it from the queue.
func (q *Queue) Peek() (interface{}, error) {
	request := codec.EncodeQueuePeekRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueuePeekResponse(response))
	}
}

// Poll retrieves and removes the head of this queue.
func (q *Queue) Poll() (interface{}, error) {
	return q.poll(context.TODO(), 0)
}

// PollWithTimeout retrieves and removes the head of this queue.
// Waits until this timeout elapses and returns the result.
func (q *Queue) PollWithTimeout(timeout time.Duration) (interface{}, error) {
	return q.poll(context.TODO(), timeout.Milliseconds())
}

// Put adds the specified element into this queue.
// If there is no space, it waits until necessary space becomes available.
func (q *Queue) Put(value interface{}) error {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return err
	} else {
		request := codec.EncodeQueuePutRequest(q.name, valueData)
		_, err := q.invokeOnPartition(context.TODO(), request, q.partitionID)
		return err
	}
}

// RemainingCapacity returns the remaining capacity of this queue.
func (q *Queue) RemainingCapacity() (int, error) {
	request := codec.EncodeQueueRemainingCapacityRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueRemainingCapacityResponse(response)), nil
	}
}

// Remove removes the specified element from the queue if it exists.
func (q *Queue) Remove(value interface{}) (bool, error) {
	if data, err := q.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueRemoveRequest(q.name, data)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeQueueRemoveResponse(response), nil
		}
	}
}

// RemoveAll removes all of the elements of the specified collection from this queue.
func (q *Queue) RemoveAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRemoveAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRemoveAllResponse(response), nil
		}
	}
}

// RemoveListener removes the specified listener.
func (q *Queue) RemoveListener(subscriptionID types.UUID) error {
	return q.listenerBinder.Remove(subscriptionID)
}

// RetainAll removes the items which are not contained in the specified collection.
func (q *Queue) RetainAll(values ...interface{}) (bool, error) {
	if valuesData, err := q.validateAndSerializeValues(values...); err != nil {
		return false, err
	} else {
		request := codec.EncodeQueueCompareAndRetainAllRequest(q.name, valuesData)
		if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeQueueCompareAndRetainAllResponse(response), nil
		}
	}
}

// Size returns the number of elements in this collection.
func (q *Queue) Size() (int, error) {
	request := codec.EncodeQueueSizeRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeQueueSizeResponse(response)), nil
	}
}

// Take retrieves and removes the head of this queue, if necessary, waits until an item becomes available.
func (q *Queue) Take() (interface{}, error) {
	request := codec.EncodeQueueTakeRequest(q.name)
	if response, err := q.invokeOnPartition(context.TODO(), request, q.partitionID); err != nil {
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

func (q *Queue) addListener(includeValue bool, handler QueueItemNotifiedHandler) (types.UUID, error) {
	subscriptionID := types.NewUUID()
	addRequest := codec.EncodeQueueAddListenerRequest(q.name, includeValue, q.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeQueueRemoveListenerRequest(q.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleQueueAddListener(msg, func(itemData serialization.Data, uuid types.UUID, eventType int32) {
			if item, err := q.convertToObject(itemData); err != nil {
				q.logger.Warnf("cannot convert data to Go value")
			} else {
				member := q.clusterService.GetMemberByUUID(uuid.String())
				handler(newQueueItemNotified(q.name, item, member, eventType))
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
