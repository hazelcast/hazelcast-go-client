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

	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
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
	return q.addWithTTL(value, 0)
}

func (q *Queue) AddWithTTL(value interface{}, ttl time.Duration) (bool, error) {
	return q.addWithTTL(value, ttl.Milliseconds())
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
		decodedValues := []interface{}{}
		for _, data := range codec.DecodeQueueDrainToResponse(response) {
			if obj, err := q.convertToObject(data); err != nil {
				return nil, err
			} else {
				decodedValues = append(decodedValues, obj)
			}
		}
		return decodedValues, nil
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

func (q *Queue) Take() (interface{}, error) {
	request := codec.EncodeQueueTakeRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return q.convertToObject(codec.DecodeQueueTakeResponse(response))
	}
}

func (q *Queue) addWithTTL(value interface{}, ttl int64) (bool, error) {
	if valueData, err := q.validateAndSerialize(value); err != nil {
		return false, nil
	} else {
		request := codec.EncodeQueueOfferRequest(q.name, valueData, ttl)
		if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeQueueOfferResponse(response), nil
		}
	}
}
