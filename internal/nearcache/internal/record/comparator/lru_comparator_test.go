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

package comparator_test

import (
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record/comparator"
	"github.com/stretchr/testify/assert"
)

func TestLRUComparator_CompareRecords(t *testing.T) {
	record1 := record.NewAbstractNearCacheRecord("value1", time.Now(), time.Now())
	record2 := record.NewAbstractNearCacheRecord("value2", time.Now(), time.Now())
	lruComp := &comparator.LRUComparator{}
	record1.SetAccessTime(time.Now())
	record1ComesFirst := lruComp.CompareRecords(record1, record2)
	assert.False(t, record1ComesFirst)

	record2.SetAccessTime(time.Now())
	record1ComesFirst = lruComp.CompareRecords(record1, record2)
	assert.True(t, record1ComesFirst)
}

func TestLRUComparator_CompareRecordsSameAccessTime(t *testing.T) {
	record1 := record.NewAbstractNearCacheRecord("value1", time.Now(), time.Now())
	record2 := record.NewAbstractNearCacheRecord("value2", time.Now(), time.Now())
	lruComp := &comparator.LRUComparator{}
	now := time.Now()
	record1.SetAccessTime(now)
	record2.SetAccessTime(now)
	record1ComesFirst := lruComp.CompareRecords(record1, record2)
	assert.True(t, record1ComesFirst)
}
