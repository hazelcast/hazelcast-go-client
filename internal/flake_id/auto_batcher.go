// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flake_id

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type IdBatchSupplier interface {
	NewIdBatch(batchSize int32) (*IdBatch, error)
}

type AutoBatcher struct {
	batchSize       int32
	validity        int64
	block           atomic.Value
	mu              sync.Mutex
	batchIdSupplier IdBatchSupplier
}

func NewAutoBatcher(batchSize int32, validity int64, supplier IdBatchSupplier) *AutoBatcher {
	autoBatcher := AutoBatcher{
		batchSize:       batchSize,
		validity:        validity,
		mu:              sync.Mutex{},
		batchIdSupplier: supplier,
	}
	autoBatcher.block.Store(NewBlock(NewIdBatch(0, 0, 0), 0)) //initialize
	return &autoBatcher
}

func (b *AutoBatcher) NewId() (int64, error) {
	for {
		block := b.block.Load().(*Block)
		res := block.next()
		if res != math.MinInt64 {
			return res, nil
		}
		b.mu.Lock()
		if block != b.block.Load().(*Block) {
			b.mu.Unlock()
			continue
		}
		idBatch, err := b.batchIdSupplier.NewIdBatch(b.batchSize)
		if err != nil {
			b.mu.Unlock()
			return 0, err
		}
		b.block.Store(NewBlock(idBatch, b.validity))
		b.mu.Unlock()
	}
}

func currentTimeInMilliSeconds() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond))
}

type Block struct {
	IdBatch      *IdBatch
	invalidSince int64
	numReturned  int32
}

func NewBlock(idBatch *IdBatch, validityMillis int64) *Block {
	block := &Block{}
	block.IdBatch = idBatch
	if validityMillis > 0 {
		block.invalidSince = validityMillis + currentTimeInMilliSeconds()
	} else {
		block.invalidSince = math.MaxInt64
	}
	return block
}

func (b *Block) next() int64 {
	if b.invalidSince <= currentTimeInMilliSeconds() {
		return math.MinInt64
	}
	var index int32
	canContinue := true
	for canContinue {
		index = atomic.LoadInt32(&b.numReturned)
		if index == b.IdBatch.batchSize {
			return math.MinInt64
		}
		if atomic.CompareAndSwapInt32(&b.numReturned, index, index+1) {
			canContinue = false
		}

	}

	return b.IdBatch.Base() + int64(index)*b.IdBatch.Increment()

}

type IdBatch struct {
	base      int64
	increment int64
	batchSize int32
}

func NewIdBatch(base int64, increment int64, batchSize int32) *IdBatch {
	return &IdBatch{
		base:      base,
		increment: increment,
		batchSize: batchSize,
	}
}

func (b *IdBatch) Base() int64 {
	return b.base
}

func (b *IdBatch) Increment() int64 {
	return b.increment
}

func (b *IdBatch) BatchSize() int32 {
	return b.batchSize
}
