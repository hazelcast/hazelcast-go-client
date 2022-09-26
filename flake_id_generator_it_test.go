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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestFlakeIDGenerator(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "BatchConcurrentCalls", f: flakeIDGeneratorBatchConcurrentCallsTest},
		{name: "BatchNextID", f: flakeIDGeneratorBatchNextIDTest},
		{name: "ExpiredBatch", f: flakeIDGeneratorExpiredBatchTest},
		{name: "IDGeneratorUsedBatch", f: flakeIDGeneratorUsedBatchTest},
		{name: "NewID", f: flakeIDGeneratorNewIDTest},
		{name: "ServiceName", f: flakeIDGeneratorServiceNameTest},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.f(t)
		})
	}
}

func flakeIDGeneratorNewIDTest(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		const idCount = 1_000
		ctx := context.Background()
		f, err := client.GetFlakeIDGenerator(ctx, it.NewUniqueObjectName("flake-id-gen"))
		if err != nil {
			t.Fatal(err)
		}
		ids := map[int64]struct{}{}
		for i := 0; i < idCount; i++ {
			if id, err := f.NewID(ctx); err != nil {
				t.Fatal(err)
			} else {
				ids[id] = struct{}{}
			}
		}
		assert.Equal(t, idCount, len(ids)) // assert uniqueness
	})
}

func flakeIDGeneratorExpiredBatchTest(t *testing.T) {
	var (
		ctx    = context.Background()
		once   = true
		expiry = time.Millisecond * 10
		batch1 = hz.NewFlakeIDBatch(0, 1, 10, expiry)
		batch2 = hz.NewFlakeIDBatch(20, 1, 10, expiry*100)
	)

	f := hz.NewFlakeIdGenerator(hz.FlakeIDGeneratorConfig{}, func(context.Context, *hz.FlakeIDGenerator) (hz.FlakeIDBatch, error) {
		if once {
			once = false
			return hz.FlakeIDBatch(batch1), nil
		}
		return hz.FlakeIDBatch(batch2), nil
	})

	id1, err := f.NewID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id1)

	time.Sleep(expiry * 2)

	id2, err := f.NewID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), id2)
}

func flakeIDGeneratorUsedBatchTest(t *testing.T) {
	var (
		ctx    = context.Background()
		once   = true
		expiry = time.Minute
		batch1 = hz.NewFlakeIDBatch(0, 1, 2, expiry)
		batch2 = hz.NewFlakeIDBatch(20, 1, 2, expiry)
	)

	f := hz.NewFlakeIdGenerator(hz.FlakeIDGeneratorConfig{}, func(context.Context, *hz.FlakeIDGenerator) (hz.FlakeIDBatch, error) {
		if once {
			once = false
			return hz.FlakeIDBatch(batch1), nil
		}
		return hz.FlakeIDBatch(batch2), nil
	})

	id1, err := f.NewID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id1)
	id2, err := f.NewID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id2)
	id3, err := f.NewID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), id3)
}

func flakeIDGeneratorBatchNextIDTest(t *testing.T) {
	testCases := []struct {
		name             string
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
			expectedSequence: []int64{10, 20, 30, hz.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             1,
			expiry:           time.Minute,
			expectedSequence: []int64{10, hz.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             0,
			expiry:           time.Minute,
			expectedSequence: []int64{hz.InvalidFlakeID},
		},
		{
			base:             10,
			increment:        10,
			size:             3,
			expiry:           -1 * time.Minute,
			expectedSequence: []int64{hz.InvalidFlakeID},
		},
	}
	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			t.Parallel()
			batch := hz.NewFlakeIDBatch(tc.base, tc.increment, tc.size, tc.expiry)
			for _, v := range tc.expectedSequence {
				assert.Equal(t, v, batch.NextID())
			}
		})
	}
}

func flakeIDGeneratorBatchConcurrentCallsTest(t *testing.T) {
	var (
		base         int64 = 0
		increment    int64 = 1
		size         int32 = 1000
		parallelism        = 10
		idPerRoutine       = 100 // equals to size/parallelism
		expiry             = time.Minute
		batch              = hz.NewFlakeIDBatch(base, increment, size, expiry)
		results            = make(chan int64, size)
	)
	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < idPerRoutine; i++ {
				results <- batch.NextID()
			}
		}()
	}
	wg.Wait()
	close(results)
	assert.Equal(t, int(size), len(results))
	lookup := map[int64]struct{}{}
	for id := range results {
		if _, ok := lookup[id]; ok {
			t.Fatalf("duplicated flake id: %d", id)
		}
		lookup[id] = struct{}{}
	}
	assert.Equal(t, int(size), len(lookup))
	assert.Equal(t, hz.InvalidFlakeID, batch.NextID())
}

func flakeIDGeneratorServiceNameTest(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		ctx := context.Background()
		name := it.NewUniqueObjectName(t.Name())
		_, err := client.GetFlakeIDGenerator(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		objs, err := client.GetDistributedObjectsInfo(ctx)
		require.NoError(t, err)
		// remove non flake ID generator proxies
		var fobjs []string
		for _, obj := range objs[:] {
			if obj.ServiceName == hz.ServiceNameFlakeIDGenerator {
				fobjs = append(fobjs, obj.Name)
			}
		}
		assert.Equal(t, 1, len(fobjs))
		assert.Equal(t, name, fobjs[0])
	})
}
