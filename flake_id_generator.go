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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

const invalidFlakeID int64 = math.MinInt64

type newFlakeIDBatchFn func(ctx context.Context, f *FlakeIDGenerator) (flakeIDBatch, error)

/*
FlakeIDGenerator is a cluster-wide unique ID generator.

Generated IDs are are k-ordered (roughly ordered) and in the range [0, math.MaxInt64].
They can be negative only if members are explicitly configured with a future epoch start value.
For details, see: https://docs.hazelcast.com/imdg/latest/data-structures/flake-id-generator.html

Instead of asking cluster for each ID, they are fetched in batches and then served.
Batch size and expiry duration can be configured via FlakeIDGeneratorConfig.
*/
type FlakeIDGenerator struct {
	*proxy
	mu         *sync.Mutex
	batch      atomic.Value
	newBatchFn newFlakeIDBatchFn
	config     FlakeIDGeneratorConfig
}

// NewID generates and returns a cluster-wide unique ID.
func (f *FlakeIDGenerator) NewID(ctx context.Context) (int64, error) {
	for {
		batch := f.batch.Load().(*flakeIDBatch)
		id := batch.nextID()
		if id != invalidFlakeID {
			return id, nil
		}
		if err := f.tryUpdateBatch(ctx, batch); err != nil {
			return 0, err
		}
	}
}

func (f *FlakeIDGenerator) tryUpdateBatch(ctx context.Context, current *flakeIDBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current != f.batch.Load().(*flakeIDBatch) {
		// batch has already been refreshed
		return nil
	}
	if b, err := f.newBatchFn(ctx, f); err != nil {
		return err
	} else {
		f.batch.Store(&b)
		return nil
	}
}

func newFlakeIdGenerator(p *proxy, config FlakeIDGeneratorConfig, newBatchFn newFlakeIDBatchFn) *FlakeIDGenerator {
	f := &FlakeIDGenerator{
		proxy:      p,
		mu:         &sync.Mutex{},
		batch:      atomic.Value{},
		newBatchFn: newBatchFn,
		config:     config,
	}
	// Store an invalid batch to fetch an actual batch lazily. The
	// very first FlakeIDGenerator.NewID call will update the batch.
	f.batch.Store(&flakeIDBatch{})
	return f
}

func flakeIDBatchFromMemberFn(ctx context.Context, f *FlakeIDGenerator) (flakeIDBatch, error) {
	request := codec.EncodeFlakeIdGeneratorNewIdBatchRequest(f.name, f.config.PrefetchCount)
	resp, err := f.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return flakeIDBatch{}, err
	}
	base, inc, size := codec.DecodeFlakeIdGeneratorNewIdBatchResponse(resp)
	return flakeIDBatch{
		base:      base,
		increment: inc,
		size:      int64(size),
		atomics:   &atomics{-1}, // to serve index 0 during atomic increment.
		expiresAt: time.Now().Add(time.Duration(f.config.PrefetchExpiry)),
	}, nil
}

// This is a separate struct because the field should be at the top: https://pkg.go.dev/sync/atomic#pkg-note-BUG
// And we don't want to suppress files on fieldAlignment check.
type atomics struct {
	index int64
}

type flakeIDBatch struct {
	expiresAt time.Time
	atomics   *atomics
	base      int64
	increment int64
	size      int64
}

func (f *flakeIDBatch) nextID() int64 {
	if time.Now().After(f.expiresAt) {
		return invalidFlakeID
	}
	idx := atomic.AddInt64(&f.atomics.index, 1)
	if idx >= f.size {
		return invalidFlakeID
	}
	return f.base + idx*f.increment
}
