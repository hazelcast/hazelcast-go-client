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
	"time"
)

// Exports non-exported types and methods to hazelcast_test package.

const (
	DefaultFlakeIDPrefetchCount  = defaultFlakeIDPrefetchCount
	DefaultFlakeIDPrefetchExpiry = defaultFlakeIDPrefetchExpiry
	InvalidFlakeID               = invalidFlakeID
)

type (
	FlakeIDBatch      flakeIDBatch
	NewFlakeIDBatchFn func(ctx context.Context, f *FlakeIDGenerator) (FlakeIDBatch, error)
)

func NewFlakeIdGenerator(config FlakeIDGeneratorConfig, newBatchFn NewFlakeIDBatchFn) *FlakeIDGenerator {
	return newFlakeIdGenerator(nil, config, func(ctx context.Context, f *FlakeIDGenerator) (flakeIDBatch, error) {
		batch, err := newBatchFn(ctx, f)
		return flakeIDBatch(batch), err
	})
}

func NewFlakeIDBatch(base, increment int64, size int32, expiry time.Duration) flakeIDBatch {
	return flakeIDBatch{
		expiresAt: time.Now().Add(expiry),
		index:     -1,
		base:      base,
		increment: increment,
		size:      int64(size),
	}
}

func (f *flakeIDBatch) NextID() int64 {
	return f.nextID()
}
