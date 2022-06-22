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
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestRingbuffer_Add(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		sequence := it.MustValue(rb.Add(context.Background(), "foo", hz.OverflowPolicyOverwrite))
		assert.Equal(t, int64(0), sequence)

		actualItem := it.MustValue(rb.ReadOne(context.Background(), 0))
		assert.Equal(t, "foo", actualItem)
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
		item, _ := rs.Get(0)
		assert.Equal(t, "0", item)
		item, _ = rs.Get(1)
		assert.Equal(t, "1", item)
		item, _ = rs.Get(2)
		assert.Equal(t, "2", item)
	})
}

func TestRingbuffer_ReadMany_GetSequence_with_startSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")

		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)
		seq, _ := rs.GetSequence(0)
		assert.Equal(t, int64(1), seq)
		seq, _ = rs.GetSequence(1)
		assert.Equal(t, int64(2), seq)
		seq, _ = rs.GetSequence(2)
		assert.Equal(t, int64(3), seq)
	})
}

func TestRingbuffer_ReadMany_Get_invalid_index(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")

		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)

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

func TestRingbuffer_ReadMany_GetSequence_invalid_index(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "x", "1", "2", "3")

		rs := it.MustValue(rb.ReadMany(context.Background(), 1, 3, 3, nil)).(hz.ReadResultSet)

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
