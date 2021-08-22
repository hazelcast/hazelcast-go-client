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

package hazelcast_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"
)

func TestFlakeIDGenerator_ExpiredBatch(t *testing.T) {
	var (
		ctx    = context.Background()
		once   = true
		expiry = time.Millisecond * 10
		batch1 = hazelcast.NewFlakeIDBatch(0, 1, 10, expiry)
		batch2 = hazelcast.NewFlakeIDBatch(20, 1, 10, expiry*100)
	)

	f := hazelcast.NewFlakeIdGenerator(hazelcast.FlakeIDGeneratorConfig{}, func(_ context.Context, _ *hazelcast.FlakeIDGenerator) (hazelcast.FlakeIDBatch, error) {
		if once {
			once = false
			return hazelcast.FlakeIDBatch(batch1), nil
		} else {
			return hazelcast.FlakeIDBatch(batch2), nil
		}
	})

	id1, err := f.NewId(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id1)

	time.Sleep(expiry * 2)

	id2, err := f.NewId(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), id2)
}

func TestFlakeIDGenerator_UsedBatch(t *testing.T) {
	var (
		ctx    = context.Background()
		once   = true
		expiry = time.Minute
		batch1 = hazelcast.NewFlakeIDBatch(0, 1, 2, expiry)
		batch2 = hazelcast.NewFlakeIDBatch(20, 1, 2, expiry)
	)

	f := hazelcast.NewFlakeIdGenerator(hazelcast.FlakeIDGeneratorConfig{}, func(_ context.Context, _ *hazelcast.FlakeIDGenerator) (hazelcast.FlakeIDBatch, error) {
		if once {
			once = false
			return hazelcast.FlakeIDBatch(batch1), nil
		} else {
			return hazelcast.FlakeIDBatch(batch2), nil
		}
	})

	id1, err := f.NewId(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id1)
	id2, err := f.NewId(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id2)
	id3, err := f.NewId(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), id3)
}

func TestFlakeIDBatch_NextID(t *testing.T) {
	testCases := []struct {
		expectedSequence []int64
		base             int64
		increment        int64
		expiry           time.Duration
		size             int32
	}{
		{
			base:             10,
			increment:        10,
			size:             3,
			expiry:           time.Minute,
			expectedSequence: []int64{10, 20, 30, hazelcast.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             1,
			expiry:           time.Minute,
			expectedSequence: []int64{10, hazelcast.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             0,
			expiry:           time.Minute,
			expectedSequence: []int64{hazelcast.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             3,
			expiry:           0,
			expectedSequence: []int64{hazelcast.InvalidFlakeID},
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			batch := hazelcast.NewFlakeIDBatch(tc.base, tc.increment, tc.size, tc.expiry)
			for _, v := range tc.expectedSequence {
				assert.Equal(t, v, batch.NextID())
			}
		})
	}
}

func TestFlakeIDBatch_ConcurrentCalls(t *testing.T) {
	var (
		base         int64 = 0
		increment    int64 = 1
		size         int32 = 1000
		parallelism        = 10
		idPerRoutine       = 100 // equals to size/parallelism
		expiry             = time.Minute
		batch              = hazelcast.NewFlakeIDBatch(base, increment, size, expiry)
		results            = make(chan int64, size)
	)
	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idPerRoutine; i++ {
				results <- batch.NextID()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int(size), len(results))
	lookup := make(map[int64]struct{})
	for i := 0; i < int(size); i++ {
		id := <-results
		if _, ok := lookup[id]; ok {
			t.Fatalf("dublicated flake id: %d", id)
		}
		lookup[id] = struct{}{}
	}
	assert.Equal(t, int(size), len(lookup))
	assert.Equal(t, hazelcast.InvalidFlakeID, batch.NextID())
}
