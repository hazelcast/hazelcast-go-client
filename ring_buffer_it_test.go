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
		sequence, err := rb.Add(context.Background(), "foo", hz.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), sequence)

		actualItem, err := rb.ReadOne(context.Background(), 0)
		assert.NoError(t, err)
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

		foo, err := rb.ReadOne(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "foo", foo)

		bar, err := rb.ReadOne(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, "bar", bar)
	})
}

func TestRingbuffer_Size(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		_, err := rb.Add(context.Background(), "one", hz.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		size, err := rb.Size(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), size)
	})
}

func TestRingbuffer_Capacity(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		capacity, err := rb.Capacity(context.Background())
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, capacity, int64(9999), "There should be a capacity of at least 9999 items.")
	})
}

func TestRingbuffer_RemainingCapacity(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		capacity, err := rb.RemainingCapacity(context.Background())
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, capacity, int64(9999), "There should be a remaining capacity of at least 9999 items.")
	})
}

func TestRingbuffer_HeadSequence_and_TailSequence(t *testing.T) {
	it.RingbufferTester(t, func(t *testing.T, rb *hz.Ringbuffer) {
		rb.AddAll(context.Background(), hz.OverflowPolicyOverwrite, "one", "two", "three")

		head, err := rb.HeadSequence(context.Background())
		assert.NoError(t, err)
		tail, err := rb.TailSequence(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(2), tail-head)
	})
}
