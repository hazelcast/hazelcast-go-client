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

package hazelcast_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestRingbuffer_Add(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		sequence := it.MustValue(rb.Add(context.Background(), "foo", hz.OverflowPolicyOverwrite))
		assert.Equal(t, int64(0), sequence)
		actualItem := it.MustValue(rb.ReadOne(context.Background(), 0))
		assert.Equal(t, "foo", actualItem)
	})
}

func TestRingbuffer_Add_OverFlowPolicyValidation(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		const notAllowedValueForPolicy = 666
		_, err := rb.Add(context.Background(), nil, notAllowedValueForPolicy)
		assert.Error(t, err)
	})
}

func TestRingbuffer_AddNilElement(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		_, err := rb.Add(context.Background(), nil, hz.OverflowPolicyFail)
		assert.Error(t, err)
	})
}

func TestRingbuffer_AddAll(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		_, err := rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "foo", "bar")
		assert.NoError(t, err)
		foo := it.MustValue(rb.ReadOne(context.Background(), 0))
		assert.Equal(t, "foo", foo)
		bar := it.MustValue(rb.ReadOne(context.Background(), 1))
		assert.Equal(t, "bar", bar)
		headSeq := it.MustValue(rb.HeadSequence(context.Background()))
		assert.Equal(t, int64(0), headSeq)
		tailSeq := it.MustValue(rb.TailSequence(context.Background()))
		assert.Equal(t, int64(1), tailSeq)
	})
}

func TestRingbuffer_AddAll_OverFlowPolicyValidation(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		const notAllowedValueForPolicy = 666
		_, err := rb.AddAll(context.Background(), notAllowedValueForPolicy, "foo", "bar")
		assert.Error(t, err)
	})
}

func TestRingbuffer_Size(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		_, err := rb.Add(context.Background(), "one", hz.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		size := it.MustValue(rb.Size(context.Background()))
		assert.Equal(t, int64(1), size)
	})
}

func TestRingbuffer_Capacity(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		capacityTotal := it.MustValue(rb.Capacity(context.Background())).(int64)
		capacityAfter := it.MustValue(rb.RemainingCapacity(context.Background()))
		assert.Equal(t, capacityTotal, capacityAfter)
		assert.True(t, capacityTotal > 0)
	})
}

func TestRingbuffer_RemainingCapacity(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		capacityBefore := it.MustValue(rb.RemainingCapacity(context.Background())).(int64)
		it.MustValue(rb.Add(context.Background(), "one", hz.OverflowPolicyOverwrite))
		capacityAfter := it.MustValue(rb.RemainingCapacity(context.Background()))
		assert.Equal(t, capacityBefore, capacityAfter, "the remaining capacity should be the capacity")
	})
}

func TestRingbuffer_HeadSequence_and_TailSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "one", "two", "three")
		head := it.MustValue(rb.HeadSequence(context.Background())).(int64)
		tail := it.MustValue(rb.TailSequence(context.Background())).(int64)
		assert.Equal(t, int64(2), tail-head)
	})
}

func TestRingbuffer_ReadMany_ReadCount(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "0", "1", "2", "x")
		rs := it.MustValue(rb.ReadMany(context.Background(), 0, 3, 3, nil)).(hz.ReadResultSet)
		assert.Equal(t, int32(3), rs.ReadCount())
	})
}

func TestRingbuffer_ReadMany_Get_with_startSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "0", "1", "2", "y", "z")
		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)
		item := it.MustValue(rs.Get(0))
		assert.Equal(t, "0", item)
		item = it.MustValue(rs.Get(1))
		assert.Equal(t, "1", item)
		item = it.MustValue(rs.Get(2))
		assert.Equal(t, "2", item)
	})
}

func TestRingbuffer_ReadMany_GetSequence_with_startSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")
		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)
		seq := it.MustValue(rs.GetSequence(0))
		assert.Equal(t, int64(1), seq)
		seq = it.MustValue(rs.GetSequence(1))
		assert.Equal(t, int64(2), seq)
		seq = it.MustValue(rs.GetSequence(2))
		assert.Equal(t, int64(3), seq)
	})
}

func TestRingbuffer_ReadMany_Get_invalid_index(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")
		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)
		assert.Equal(t, int32(3), rs.ReadCount())
		item, err := rs.Get(999)
		assert.Nil(t, item)
		assert.Error(t, err)
		_, err = rs.Get(-1)
		assert.Error(t, err)
		_, err = rs.Get(3)
		assert.Error(t, err)
		_, err = rs.Get(2)
		assert.NoError(t, err)
	})
}

// readManyAsync_whenStartSequenceIsNegative
func TestRingbuffer_ReadMany_NonNegativeParameterValidation(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")
		_, err := rb.ReadMany(context.Background(), -1, 0, 0, nil)
		assert.Error(t, err)
		_, err = rb.ReadMany(context.Background(), 0, -1, 0, nil)
		assert.Error(t, err)
		_, err = rb.ReadMany(context.Background(), 0, 0, -1, nil)
		assert.Error(t, err)
	})
}

func TestRingbuffer_ReadMany_InvalidParameterValidation(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")
		_, err := rb.ReadMany(context.Background(), 0, 9, 1, nil)
		assert.Error(t, err, "minCount should be smaller maxCount")
		_, err = rb.ReadMany(context.Background(), 0, 0, hz.MaxBatchSize+1, nil)
		assert.Error(t, err, "maxCount should be smaller or equal MaxBatchSize")
		capacity := it.MustValue(rb.Capacity(context.Background())).(int64)
		_, err = rb.ReadMany(context.Background(), 0, 0, int32(capacity+1), nil)
		assert.Error(t, err, "maxCount should be smaller or equal capacity")
	})
}

func TestRingbuffer_ReadMany_GetSequence_invalid_index(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx := context.Background()
		rb.AddAll(ctx, hz.OverflowPolicyOverwrite, "x", "1", "2", "3")
		rs := it.MustValue(rb.ReadMany(ctx, 1, 3, 3, nil)).(hz.ReadResultSet)
		seq, err := rs.GetSequence(999)
		assert.Equal(t, int64(-1), seq)
		assert.Error(t, err)
		_, err = rs.GetSequence(-1)
		assert.Error(t, err)
		_, err = rs.GetSequence(3)
		assert.Error(t, err)
		_, err = rs.GetSequence(2)
		assert.NoError(t, err)
	})
}

func TestRingbuffer_ReadMany_WhenStartSequenceIsNoLongerAvailable_GetsClamped(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx := context.Background()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		require.NoError(t, err)
		rs, err := rb.ReadMany(ctx, 0, 1, 10, nil)
		require.NoError(t, err)
		require.Equal(t, int32(10), rs.ReadCount())
		e, err := rs.Get(0)
		require.NoError(t, err)
		require.Equal(t, "1", e)
		e, err = rs.Get(9)
		require.NoError(t, err)
		require.Equal(t, "10", e)
	})
}

func TestRingBuffer_ReadMany_WhenStartSequenceIsEqualToTailSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx := context.Background()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		require.NoError(t, err)
		rs, err := rb.ReadMany(ctx, 10, 1, 10, nil)
		require.NoError(t, err)
		require.Equal(t, int32(1), rs.ReadCount())
		e, err := rs.Get(0)
		require.NoError(t, err)
		require.Equal(t, "10", e)
	})
}

func TestRingBuffer_ReadMany_WhenStartSequenceIsJustBeyondTailSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		require.NoError(t, err)
		_, err = rb.ReadMany(ctx, 11, 1, 10, nil)
		require.Errorf(t, err, "context deadline exceeded")
	})
}

func TestRingBuffer_ReadMany_WhenStartSequenceIsWellBeyondTailSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
		require.NoError(t, err)
		_, err = rb.ReadMany(ctx, 19, 1, 10, nil)
		require.Errorf(t, err, "context deadline exceeded")
	})
}

type PrefixFilter struct {
	Prefix string
}

func (p *PrefixFilter) FactoryID() int32 {
	return 666
}

func (p *PrefixFilter) ClassID() int32 {
	return 14
}

func (p *PrefixFilter) WriteData(output serialization.DataOutput) {
	output.WriteString(p.Prefix)
}

func (p *PrefixFilter) ReadData(input serialization.DataInput) {
	p.Prefix = input.ReadString()
}

type IdentifiedFactory struct{}

func (f IdentifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == 14 {
		return &PrefixFilter{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", id))
}

func (f IdentifiedFactory) FactoryID() int32 {
	return 666
}

func TestRingBuffer_ReadMany_WithFilter(t *testing.T) {
	it.RingbufferTesterWithConfigAndName(t, func() string {
		return t.Name()
	}, func(config *hz.Config) {
		config.Serialization.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	}, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx := context.Background()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite,
			"good1", "bad1",
			"good2", "bad2",
			"good3", "bad3")
		require.NoError(t, err)
		rs, err := rb.ReadMany(ctx, 0, 3, 3, &PrefixFilter{Prefix: "good"})
		require.NoError(t, err)
		// since it reads 5 values to reach 3 (maxCount) values starting with "good"
		require.Equal(t, int32(5), rs.ReadCount())
		require.Equal(t, "good1", it.MustValue(rs.Get(0)))
		require.Equal(t, "good2", it.MustValue(rs.Get(1)))
		require.Equal(t, "good3", it.MustValue(rs.Get(2)))
	})
}

func TestRingBuffer_ReadMany_WithFilter_AndMaxCount(t *testing.T) {
	it.RingbufferTesterWithConfigAndName(t, func() string {
		return t.Name()
	}, func(config *hz.Config) {
		config.Serialization.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	}, func(t *testing.T, rb *hz.Ringbuffer) {
		ctx := context.Background()
		_, err := rb.AddAll(ctx, hz.OverflowPolicyOverwrite,
			"good1", "bad1",
			"good2", "bad2",
			"good3", "bad3",
			"good4", "bad4")
		require.NoError(t, err)
		rs, err := rb.ReadMany(ctx, 0, 3, 3, &PrefixFilter{Prefix: "good"})
		require.NoError(t, err)
		// since it reads 5 values to reach 3 (maxCount) values starting with "good"
		require.Equal(t, int32(5), rs.ReadCount())
		require.Equal(t, "good1", it.MustValue(rs.Get(0)))
		require.Equal(t, "good2", it.MustValue(rs.Get(1)))
		require.Equal(t, "good3", it.MustValue(rs.Get(2)))
	})
}
