/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// A RingBuffer is a data structure where the content is stored in a ring-like
// structure. A RingBuffer has a fixed capacity, so it won't grow beyond
// that capacity and endanger the stability of the system. If that capacity
// is exceeded, the oldest item in the RingBuffer is overwritten.
//
// For details see https://docs.hazelcast.com/imdg/latest/data-structures/ringbuffer
type RingBuffer struct {
	*proxy
	partitionID int32
}

func newRingBuffer(p *proxy) (*RingBuffer, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &RingBuffer{proxy: p, partitionID: partitionID}, nil
	}
}

// Add adds an item to the tail of this ringbuffer. The overflowPolicy determines what will happen,
// if there is no space left in this ringbuffer. If OverflowPolicyOverwrite was passed,
// the new item will overwrite the oldest one regardless of the configured time-to-live.
//
// In the case when OverflowPolicyFail was specified, the add operation will keep failing until an oldest item in this
// ringbuffer will reach its time-to-live.
//
// Add returns the sequence number of the added item. You can read the added item using this number.
func (rb *RingBuffer) Add(ctx context.Context, item interface{}, overflowPolicy types.OverflowPolicy) (sequence int64, err error) {
	elementData, err := rb.validateAndSerialize(item)
	if err != nil {
		return -1, err
	}
	request := codec.EncodeRingBufferAddRequest(rb.name, overflowPolicy, elementData)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return -1, err
	}
	return codec.DecodeRingBufferAddResponse(response), nil
}
