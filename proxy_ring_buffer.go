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
)

// A Ringbuffer is a data structure where the content is stored in a ring-like
// structure. A Ringbuffer has a fixed capacity, so it won't grow beyond
// that capacity and endanger the stability of the system. If that capacity
// is exceeded, the oldest item in the Ringbuffer is overwritten.
//
// For details see https://docs.hazelcast.com/imdg/latest/data-structures/ringbuffer
type Ringbuffer struct {
	*proxy
	partitionID int32
}

// OverflowPolicy
// Using this OverflowPolicy one can control the behavior what should to be done
// when an item is about to be added to the RingBuffer, but there is {@code 0}
// remaining capacity.
//
// Overflowing happens when a time-to-live is set and the oldest item in
// the RingBuffer (the head) is not old enough to expire.
type OverflowPolicy int
type ReadResultSet struct{}

const (
	// OverflowPolicyOverwrite
	// Using this OverflowPolicyOverwrite policy the oldest item is overwritten
	// no matter it is not old enough to retire. Using this policy you are
	// sacrificing the time-to-live in favor of being able to write.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full
	// and the oldest item in the ring has been placed a second ago, then there
	// are 29 seconds remaining for that item. Using this policy you are going
	// to overwrite no matter what.
	OverflowPolicyOverwrite OverflowPolicy = 0

	// OverflowPolicyFail
	// Using this policy the call will fail immediately and the oldest item will
	// not be overwritten before it is old enough to retire. So this policy
	// sacrificing the ability to write in favor of time-to-live.
	//
	// The advantage of OverflowPolicyFail is that the caller can decide what to do
	// since it doesn't trap the thread due to backoff.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full
	// and the oldest item in the ring has been placed a second ago, then there
	// are 29 seconds remaining for that item. Using this policy you are not
	// going to overwrite that item for the next 29 seconds.
	OverflowPolicyFail OverflowPolicy = 1

	// ReadResultSetSequenceUnavailable is used when error happened
	ReadResultSetSequenceUnavailable int64 = -1
)

func newRingbuffer(p *proxy) (*Ringbuffer, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &Ringbuffer{proxy: p, partitionID: partitionID}, nil
	}
}

// Add an item to the tail of the Ringbuffer. If there is space in the Ringbuffer, the call
// will return the sequence of the written item. If there is no space, it depends on the overflow policy what happens:
// OverflowPolicy OverflowPolicyOverwrite  we just overwrite the oldest item in the Ringbuffer, and we violate the ttl
// OverflowPolicy FAIL we return -1. The reason that FAIL exist is to give the opportunity to obey the ttl.
//
// This sequence will always be unique for this Ringbuffer instance, so it can be used as a unique id generator if you are
// publishing items on this Ringbuffer. However, you need to take care of correctly determining an initial id when any node
// uses the Ringbuffer for the first time. The most reliable way to do that is to write a dummy item into the Ringbuffer and
// use the returned sequence as initial  id. On the reading side, this dummy item should be discarded. Please keep in mind that
// this id is not the sequence of the item you are about to publish but from a previously published item. So it can't be used
// to find that item.
//
// Add returns the sequence number of the added item. You can read the added item using this number.
func (rb *Ringbuffer) Add(ctx context.Context, item interface{}, overflowPolicy OverflowPolicy) (sequence int64, err error) {
	elementData, err := rb.validateAndSerialize(item)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	request := codec.EncodeRingbufferAddRequest(rb.name, int32(overflowPolicy), elementData)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferAddResponse(response), nil
}

// AddAll
// Adds all the items of a collection to the tail of the Ringbuffer. A addAll is likely to outperform multiple calls
// to add(Object) due to better io utilization and a reduced number of executed operations. If the batch is empty,
// the call is ignored. When the collection is not empty, the content is copied into a different data-structure.
// This means that: after this call completes, the collection can be re-used. the collection doesn't need to be serializable.
// If the collection is larger than the capacity of the Ringbuffer, then the items that were written first will be
// overwritten. Therefor this call will not block. The items are inserted in the order of the Iterator of the collection.
// If an addAll is executed concurrently with an add or addAll, no guarantee is given that items are contiguous.
// The result of the future contains the sequenceId of the last written item.
func (rb *Ringbuffer) AddAll(ctx context.Context, overflowPolicy OverflowPolicy, items ...interface{}) (int64, error) {
	elementData, err := rb.validateAndSerializeValues(items)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	request := codec.EncodeRingbufferAddAllRequest(rb.name, elementData, int32(overflowPolicy))
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferAddAllResponse(response), nil
}

// ReadOne
// Reads one item from the Ringbuffer. If the sequence is one beyond the current tail, this call blocks until an
// item is added. This method is not destructive unlike e.g. a queue.take. So the same item can be read by multiple
// readers, or it can be read multiple times by the same reader. Currently, it isn't possible to control how long this
// call is going to block. In the future we could add e.g. tryReadOne(long sequence, long timeout, TimeUnit unit).
func (rb *Ringbuffer) ReadOne(ctx context.Context, sequence int64) (interface{}, error) {
	request := codec.EncodeRingbufferReadOneRequest(rb.name, sequence)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return rb.convertToObject(codec.DecodeRingbufferReadOneResponse(response))
}

// Capacity returns the capacity of this Ringbuffer.
func (rb *Ringbuffer) Capacity(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferCapacityRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferCapacityResponse(response), nil
}

// Size returns number of items in the Ringbuffer. If no ttl is set, the size will always be equal to capacity after the
// head completed the first loop-around the ring. This is because no items are getting retired.
func (rb *Ringbuffer) Size(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferSizeRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferSizeResponse(response), nil
}

// TailSequence returns the sequence of the tail. The tail is the side of the ringbuffer where the items are added to.
// The initial value of the tail is -1.
func (rb *Ringbuffer) TailSequence(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferTailSequenceRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferTailSequenceResponse(response), nil
}

// HeadSequence returns the sequence of the head. The head is the side of the ringbuffer where the oldest items in the ringbuffer
// are found. If the RingBuffer is empty, the head will be one more than the tail.
// The initial value of the head is 0 (1 more than tail).
func (rb *Ringbuffer) HeadSequence(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferHeadSequenceRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferHeadSequenceResponse(response), nil
}

// RemainingCapacity
// Returns the remaining capacity of the Ringbuffer. The returned value could be stale as soon as it is returned.
// If ttl is not set, the remaining capacity will always be the capacity.
func (rb *Ringbuffer) RemainingCapacity(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferRemainingCapacityRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferRemainingCapacityResponse(response), nil
}

func (rb *Ringbuffer) ReadMany(ctx context.Context, startSequence int64, minCount int64, maxCount int64, filter interface{}) (ReadResultSet, error) {
	// TODO nitram509
	return ReadResultSet{}, nil
}

func (rrs *ReadResultSet) ReadCount() int32 {
	// TODO nitram509
	return 0
}

func (rrs *ReadResultSet) Get(index int) (interface{}, error) {
	// TODO nitram509
	return nil, nil
}

func (rrs *ReadResultSet) GetSequence(index int) (int64, error) {
	// TODO nitram509
	return 0, nil
}

func (rrs *ReadResultSet) Size() int {
	// TODO nitram509
	return 0
}

func (rrs *ReadResultSet) GetNextSequenceToReadFrom() int64 {
	// TODO nitram509
	return 0
}
