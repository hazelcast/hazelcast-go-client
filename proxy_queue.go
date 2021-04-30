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

func (q *Queue) Offer(value interface{}) (bool, error) {
	return q.offerWithTTL(value, 0)
}

func (q *Queue) OfferWithTTL(value interface{}, ttl time.Duration) (bool, error) {
	return q.offerWithTTL(value, ttl.Milliseconds())
}

func (q *Queue) Take() (interface{}, error) {
	request := codec.EncodeQueueTakeRequest(q.name)
	if response, err := q.invokeOnPartition(context.Background(), request, q.partitionID); err != nil {
		return nil, err
	} else {
		return codec.DecodeQueueTakeResponse(response), nil
	}
}

func (q *Queue) offerWithTTL(value interface{}, ttl int64) (bool, error) {
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
