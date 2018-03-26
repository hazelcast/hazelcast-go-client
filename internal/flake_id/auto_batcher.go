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
	block           *Block
	mu              sync.Mutex
	batchIdSupplier IdBatchSupplier
}

func NewAutoBatcher(batchSize int32, validity int64, supplier IdBatchSupplier) *AutoBatcher {
	autoBatcher := AutoBatcher{
		batchSize:       batchSize,
		validity:        validity,
		mu:              sync.Mutex{},
		block:           NewBlock(NewIdBatch(0, 0, 0), 0),
		batchIdSupplier: supplier,
	}
	return &autoBatcher
}

func (self *AutoBatcher) NewId() (int64, error) {
	for {
		self.mu.Lock()
		block := self.block
		self.mu.Unlock()
		res := block.next()
		if res != math.MinInt64 {
			return res, nil
		}
		self.mu.Lock()
		if block != self.block {
			self.mu.Unlock()
			continue
		}
		idBatch, err := self.batchIdSupplier.NewIdBatch(self.batchSize)
		if err != nil {
			self.mu.Unlock()
			return 0, err
		}
		self.block = NewBlock(idBatch, self.validity)
		self.mu.Unlock()
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

func (self *Block) next() int64 {
	if self.invalidSince <= currentTimeInMilliSeconds() {
		return math.MinInt64
	}
	var index int32
	canContinue := true
	for canContinue {
		index = atomic.LoadInt32(&self.numReturned)
		if index == self.IdBatch.batchSize {
			return math.MinInt64
		}
		if atomic.CompareAndSwapInt32(&self.numReturned, index, index+1) {
			canContinue = false
		}

	}

	return self.IdBatch.Base() + int64(index)*self.IdBatch.Increment()

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

func (self *IdBatch) Base() int64 {
	return self.base
}

func (self *IdBatch) Increment() int64 {
	return self.increment
}

func (self *IdBatch) BatchSize() int32 {
	return self.batchSize
}
