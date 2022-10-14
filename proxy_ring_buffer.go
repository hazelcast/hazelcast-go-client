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
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

/*
A Ringbuffer is a data structure where the content is stored in a ring-like structure.
A Ringbuffer has a capacity ,so it won't grow beyond that capacity and endanger the stability of the system.
If that capacity is exceeded, then the oldest item in the Ringbuffer is overwritten.
For details, see https://docs.hazelcast.com/hazelcast/latest/data-structures/ringbuffer

The Ringbuffer has two always-incrementing sequences:
- TailSequence: This is the side where the youngest item is found.
	So the tail is the side of the Ringbuffer where items are added to.
- HeadSequence: This is the side where the oldest items are found.
	So the head is the side where items get discarded.

The items in the Ringbuffer can be found by a sequence that is in between (inclusive) the head and tail sequence.

If data is read from a Ringbuffer with a sequence that is smaller than thehead sequence, it means that the data is not available anymore and a hzerrors.ErrStaleSequence is returned.

A Ringbuffer currently is a replicated, but not partitioned data structure.
So all data is stored in a single partition, similarly to the Queue implementation.

A Ringbuffer can be used in a way similar to the Queue, but one of the key differences is that a Queue.take is destructive, meaning that only 1 thread is able to take an item.
A ReadOne is not destructive, so you can have multiple threads reading the same item multiple times.
*/
type Ringbuffer struct {
	*proxy
	partitionID int32
	capacity    int64
}

// ReadResultSet is used as return type in ReadMany() operations from a Ringbuffer
type ReadResultSet struct {
	readCount        int32
	conversionErrors []error
	convertedItems   []interface{}
	itemSeqs         []int64
	nextSeq          int64
}

// ReadResultSetSequenceUnavailable is used when error happened
const ReadResultSetSequenceUnavailable int64 = -1

const MaxBatchSize = 1000

// OverflowPolicy
// Using this OverflowPolicy one can control the behavior what should to be done when an item is about to be added to the Ringbuffer, but there is {@code 0} remaining capacity.
// Overflowing happens when a time-to-live is set and the oldest item in the Ringbuffer (the head) is not old enough to expire.
type OverflowPolicy int

const (
	// OverflowPolicyOverwrite
	// Using this OverflowPolicyOverwrite policy the oldest item is overwritten no matter it is not old enough to retire.
	// Using this policy you are sacrificing the time-to-live in favor of being able to write.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full and the oldest item in the ring has been placed a second ago, then there are 29 seconds remaining for that item.
	// Using this policy you are going to overwrite no matter what.
	OverflowPolicyOverwrite OverflowPolicy = 0
	// OverflowPolicyFail
	// Using this policy the call will fail immediately and the oldest item will not be overwritten before it is old enough to retire.
	// So this policy sacrificing the ability to write in favor of time-to-live.
	//
	// The advantage of OverflowPolicyFail is that the caller can decide what to do since it doesn't trap the thread due to backoff.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full and the oldest item in the ring has been placed a second ago, then there are 29 seconds remaining for that item.
	// Using this policy you are not going to overwrite that item for the next 29 seconds.
	OverflowPolicyFail OverflowPolicy = 1
)

func newRingbuffer(p *proxy) (*Ringbuffer, error) {
	partitionID, err := p.stringToPartitionID(p.name)
	if err != nil {
		return nil, err
	}
	rb := &Ringbuffer{
		proxy:       p,
		partitionID: partitionID,
		capacity:    ReadResultSetSequenceUnavailable,
	}
	return rb, nil
}

// Add adds an item to the tail of the Ringbuffer.
// If there is space in the Ringbuffer, the call will return the sequence of the written item.
// If there is no space, it depends on the overflow policy what happens:
// - OverflowPolicyOverwrite:  we just overwrite the oldest item in the Ringbuffer, and we violate the ttl
// - OverflowPolicyFail: FAIL we return -1. The reason that FAIL exist is to give the opportunity to obey the ttl.
//
// This sequence will always be unique for this Ringbuffer instance, so it can be used as a unique id generator if you are
// publishing items on this Ringbuffer.
// However, you need to take care of correctly determining an initial id when any node
// uses the Ringbuffer for the first time.
// The most reliable way to do that is to write a dummy item into the Ringbuffer and use the returned sequence as initial  id.
// On the reading side, this dummy item should be discarded.
// Please keep in mind that this id is not the sequence of the item you are about to publish but from a previously published item.
// So it can't be used to find that item.
//
// Add returns the sequence number of the added item.
// You can read the added item using this number.
func (rb *Ringbuffer) Add(ctx context.Context, item interface{}, overflowPolicy OverflowPolicy) (sequence int64, err error) {
	if overflowPolicy != OverflowPolicyFail && overflowPolicy != OverflowPolicyOverwrite {
		msg := fmt.Sprintf("invalid overflow policy: %d", overflowPolicy)
		return 0, ihzerrors.NewIllegalArgumentError(msg, nil)
	}
	serializedItemData, err := rb.validateAndSerialize(item)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	request := codec.EncodeRingbufferAddRequest(rb.name, int32(overflowPolicy), serializedItemData)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferAddResponse(response), nil
}

// AddAll
// Adds all the items of a collection to the tail of the Ringbuffer.
// A addAll is likely to outperform multiple calls to add(Object) due to better io utilization and a reduced number of executed operations.
// If the batch is empty, the call is ignored.
// When the slice is not empty, the content is copied into a different data-structure.
// This means that: after this call completes, the slice can be re-used.
// If the slice is larger than the capacity of the Ringbuffer, then the items that were written first will be overwritten.
// Therefore, this call will not block.
// The items are inserted in the order of the slice.
// If an addAll is executed concurrently with an add or addAll, no guarantee is given that items are contiguous.
// The result contains the sequenceId of the last written item.
func (rb *Ringbuffer) AddAll(ctx context.Context, overflowPolicy OverflowPolicy, items ...interface{}) (int64, error) {
	if overflowPolicy != OverflowPolicyFail && overflowPolicy != OverflowPolicyOverwrite {
		msg := fmt.Sprintf("invalid overflow policy: %d", overflowPolicy)
		return 0, ihzerrors.NewIllegalArgumentError(msg, nil)
	}
	serializedItemsData, err := rb.validateAndSerializeValues(items)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	request := codec.EncodeRingbufferAddAllRequest(rb.name, serializedItemsData, int32(overflowPolicy))
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferAddAllResponse(response), nil
}

// ReadOne
// Reads one item from the Ringbuffer.
// If the sequence is one beyond the current tail, this call blocks until an item is added.
// This method is not destructive unlike e.g. a queue.take.
// So the same item can be read by multiple readers, or it can be read multiple times by the same reader.
func (rb *Ringbuffer) ReadOne(ctx context.Context, sequence int64) (interface{}, error) {
	request := codec.EncodeRingbufferReadOneRequest(rb.name, sequence)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return nil, err
	}
	return rb.convertToObject(codec.DecodeRingbufferReadOneResponse(response))
}

// Capacity returns the capacity of this Ringbuffer.
func (rb *Ringbuffer) Capacity(ctx context.Context) (int64, error) {
	if rb.capacity == ReadResultSetSequenceUnavailable {
		request := codec.EncodeRingbufferCapacityRequest(rb.name)
		response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
		if err != nil {
			return ReadResultSetSequenceUnavailable, err
		}
		rb.capacity = codec.DecodeRingbufferCapacityResponse(response)
	}
	return rb.capacity, nil
}

// Size returns number of items in the Ringbuffer.
// If no ttl is set, the size will always be equal to capacity after the head completed the first loop-around the ring.
// This is because no items are getting retired.
func (rb *Ringbuffer) Size(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferSizeRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferSizeResponse(response), nil
}

// TailSequence returns the sequence of the tail.
// The tail is the side of the Ringbuffer where the items are added to.
// The initial value of the tail is -1.
func (rb *Ringbuffer) TailSequence(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferTailSequenceRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferTailSequenceResponse(response), nil
}

// HeadSequence returns the sequence of the head.
// The head is the side of the Ringbuffer where the oldest items in the Ringbuffer are found.
// If the Ringbuffer is empty, the head will be one more than the tail.
// The initial value of the head is 0 (1 more than tail).
func (rb *Ringbuffer) HeadSequence(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferHeadSequenceRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferHeadSequenceResponse(response), nil
}

// RemainingCapacity returns the remaining capacity of the Ringbuffer.
// The returned value could be stale as soon as it is returned.
// If ttl is not set, the remaining capacity will always be the capacity.
func (rb *Ringbuffer) RemainingCapacity(ctx context.Context) (int64, error) {
	request := codec.EncodeRingbufferRemainingCapacityRequest(rb.name)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return codec.DecodeRingbufferRemainingCapacityResponse(response), nil
}

// ReadMany reads a batch of items from the Ringbuffer.
// If the number of available items after the first read item is smaller than the maxCount, these items are returned.
// So it could be the number of items read is smaller than the maxCount.
// If there are fewer items available than minCount, then this call blacks.
// These blocking calls consume server memory and if there are many calls, it can be possible to see leaking memory or OOME.
// Reading a batch of items is likely to perform better because less overhead is involved.
// A filter can be provided to only select items that need to be read.
// If the filter is nil, all items are read.
// If the filter is not null, only items where the filter function returns true are returned.
// Using filters is a good way to prevent getting items that are of no value to the receiver.
// This reduces the amount of IO and the number of operations being executed, and can result in a significant performance improvement.
// If the startSequence is smaller than the smallest sequence still available in the Ringbuffer (HeadSequence}, then the smallest available sequence will be used as the start sequence and the minimum/maximum number of items will be attempted to be read from there on.
// If the startSequence is bigger than the last available sequence in the Ringbuffer (TailSequence), then the last available sequence plus one will be used as the start sequence and the call will block until further items become available and it can read at least the minimum number of items.
func (rb *Ringbuffer) ReadMany(ctx context.Context, startSequence int64, minCount int32, maxCount int32, filter interface{}) (ReadResultSet, error) {
	if startSequence < 0 {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError("startSequence can't be smaller then 0", nil)
	}
	if minCount < 0 {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError("minCount can't be smaller then 0", nil)
	}
	if maxCount < 0 {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError("maxCount can't be smaller then 0", nil)
	}
	if maxCount < minCount {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError("maxCount should be equal or larger than minCount", nil)
	}
	if maxCount > MaxBatchSize {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("maxCount can't be larger than %d", MaxBatchSize), nil)
	}
	capacity, err := rb.Capacity(context.Background())
	if int64(maxCount) > capacity || err != nil {
		return ReadResultSet{}, ihzerrors.NewIllegalArgumentError("the maxCount should be smaller than or equal to the capacity", err)
	}
	var serializedFilterData iserialization.Data
	if filter != nil {
		data, err := rb.validateAndSerialize(filter)
		serializedFilterData = data
		if err != nil {
			return ReadResultSet{}, err
		}
	}
	request := codec.EncodeRingbufferReadManyRequest(rb.name, startSequence, minCount, maxCount, serializedFilterData)
	response, err := rb.invokeOnPartition(ctx, request, rb.partitionID)
	if err != nil {
		return ReadResultSet{}, err
	}
	readCount, items, itemSeqs, nextSeq := codec.DecodeRingbufferReadManyResponse(response)
	var errors = make([]error, len(items))
	var convertedItems = make([]interface{}, len(items))
	for i := 0; i < len(items); i++ {
		convertedItems[i], errors[i] = rb.convertToObject(items[i])
	}
	return ReadResultSet{
		readCount:        readCount,
		conversionErrors: errors,
		convertedItems:   convertedItems,
		itemSeqs:         itemSeqs,
		nextSeq:          nextSeq,
	}, nil
}

// ReadCount returns the number of items that have been read before filtering.
// If no filter is set, then the ReadCount will be equal to Size.
// But if a filter is applied, it could be that items are read, but are filtered out.
// So if you are trying to make another read based on the ReadResultSet, then you should increment the sequence by ReadCount and not by Size.
// Otherwise, you will be re-reading the filtered messages.
func (rrs *ReadResultSet) ReadCount() int32 {
	return rrs.readCount
}

// Get gets the item at the given index.
func (rrs *ReadResultSet) Get(index int) (interface{}, error) {
	if err := check.WithinRangeInt32(int32(index), 0, int32(len(rrs.convertedItems)-1)); err != nil {
		return nil, err
	}
	return rrs.convertedItems[index], rrs.conversionErrors[index]
}

// GetSequence returns the sequence number for the item at the given index.
func (rrs *ReadResultSet) GetSequence(index int) (int64, error) {
	if err := check.WithinRangeInt32(int32(index), 0, int32(len(rrs.itemSeqs)-1)); err != nil {
		return ReadResultSetSequenceUnavailable, err
	}
	return rrs.itemSeqs[index], nil
}

// Size returns the result set size.
// See also ReadCount.
func (rrs *ReadResultSet) Size() int {
	return len(rrs.convertedItems)
}

// GetNextSequenceToReadFrom returns sequence number of the item following the last read item.
// This sequence can then be used to read items following the ones returned by this result set.
// Usually this sequence is equal to the sequence used to retrieve this result set incremented by the ReadCount.
// In cases when the reader tolerates lost items, this is not the case.
// For instance, if the reader requests an item with a stale sequence (one which has already been overwritten), the read will jump to the oldest sequence and read from there.
// Similarly, if the reader requests an item in the future (e.g. because the partition was lost and the reader was unaware of this), the read method will jump back to the newest available sequence.
// Because of these jumps and only in the case when the reader is loss tolerant, the next sequence must be retrieved using this method.
// A return value of ReadResultSetSequenceUnavailable means that the information is not available.
func (rrs *ReadResultSet) GetNextSequenceToReadFrom() int64 {
	return rrs.nextSeq
}
