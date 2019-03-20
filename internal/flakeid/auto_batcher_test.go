// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package flakeid

import (
	"testing"
	"time"
)

type testSupplier struct {
	base int64
}

func (s *testSupplier) NewIDBatch(batchSize int32) (*IDBatch, error) {

	batch := NewIDBatch(s.base, 1, batchSize)
	s.base += int64(batchSize)
	return batch, nil
}

var batcher *AutoBatcher

func TestMain(t *testing.M) {
	t.Run()
}

func TestAutoBatcher_WhenValidButUsedAllThenFetchNew(t *testing.T) {
	batcher = NewAutoBatcher(3, 10000, &testSupplier{})
	if id, _ := batcher.NewID(); id != 0 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 0, id)
	}
	if id, _ := batcher.NewID(); id != 1 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 1, id)
	}
	if id, _ := batcher.NewID(); id != 2 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 2, id)
	}
	if id, _ := batcher.NewID(); id != 3 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 3, id)
	}
}

func TestAutoBatcher_WhenExpiredThenFetchNew(t *testing.T) {
	batcher = NewAutoBatcher(3, 10000, &testSupplier{})
	if id, _ := batcher.NewID(); id != 0 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 0, id)
	}
	time.Sleep(10000 * time.Millisecond)
	if id, _ := batcher.NewID(); id != 3 {
		t.Errorf("AutoBatcher failed expected: %d got %d", 3, id)
	}
}
