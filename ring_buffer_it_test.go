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
	"github.com/hazelcast/hazelcast-go-client/types"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestRingBuffer_Add(t *testing.T) {
	it.RingBufferTester(t, func(t *testing.T, rb *hz.RingBuffer) {
		sequence, err := rb.Add(context.Background(), "test1", types.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), sequence)

		sequence, err = rb.Add(context.Background(), "test2", types.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), sequence)

		sequence, err = rb.Add(context.Background(), "test2", types.OverflowPolicyOverwrite)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), sequence)
	})
}

func TestRingBuffer_AddNilElement(t *testing.T) {
	it.RingBufferTester(t, func(t *testing.T, rb *hz.RingBuffer) {
		_, err := rb.Add(context.Background(), nil, types.OverflowPolicyOverwrite)
		assert.Error(t, err)
	})
}
