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

package nearcache_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	inearcache "github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

func TestLRUComparatorDoesNotPrematurelySelectNewlyCreatedEntries(t *testing.T) {
	// 1. Create expected list of ordered elements by
	// sorting entries based on their idle-times. Longest
	// idle time must be the first element of the list.
	// 2. Then sort given entries by using LRU eviction comparator.
	const now = 20
	given := []nearcache.EvictableEntryView{
		{CreationTime: 1, LastAccessTime: 0},
		{CreationTime: 2, LastAccessTime: 3},
		{CreationTime: 2, LastAccessTime: 0},
		{CreationTime: 4, LastAccessTime: 4},
		{CreationTime: 5, LastAccessTime: 20},
		{CreationTime: 6, LastAccessTime: 6},
		{CreationTime: 7, LastAccessTime: 0},
		{CreationTime: 9, LastAccessTime: 15},
		{CreationTime: 10, LastAccessTime: 10},
		{CreationTime: 10, LastAccessTime: 0},
	}
	sortFn := func(sorted []nearcache.EvictableEntryView) func(i, j int) bool {
		return func(i, j int) bool {
			a := sorted[i]
			b := sorted[j]
			return idleTime(now, a) > idleTime(now, b)
		}
	}
	evictionPolicyHelper(t, given, sortFn, inearcache.LRUEvictionPolicyComparator)
}

func TestLFUEvictionPolicyComparator(t *testing.T) {
	given := []nearcache.EvictableEntryView{
		{Hits: 5, CreationTime: 2},
		{Hits: 10, CreationTime: 5},
		{Hits: 2, CreationTime: 7},
		{Hits: 5, CreationTime: 4},
	}
	sortFn := func(sorted []nearcache.EvictableEntryView) func(i, j int) bool {
		return func(i, j int) bool {
			a := sorted[i]
			b := sorted[j]
			if a.Hits < b.Hits {
				return true
			}
			if a.Hits > b.Hits {
				return false
			}
			return a.CreationTime < b.CreationTime
		}
	}
	evictionPolicyHelper(t, given, sortFn, inearcache.LFUEvictionPolicyComparator)
}

func TestRandomEvictionPolicyComparator(t *testing.T) {
	given := []nearcache.EvictableEntryView{
		{Hits: 5},
		{Hits: 10},
		{Hits: 2},
		{Hits: 7},
	}
	target := make([]nearcache.EvictableEntryView, len(given))
	copy(target, given)
	sort.Slice(given, func(i, j int) bool {
		a := given[i]
		b := given[j]
		return inearcache.RandomEvictionPolicyComparator.Compare(a, b) < 0
	})
	assert.Equal(t, target, given)
}

func evictionPolicyHelper(t *testing.T, given []nearcache.EvictableEntryView, sortGenFn func(sorted []nearcache.EvictableEntryView) func(i, j int) bool, cmp nearcache.EvictionPolicyComparator) {
	sorted := make([]nearcache.EvictableEntryView, len(given))
	copy(sorted, given)
	sortFn := sortGenFn(sorted)
	sort.Slice(sorted, sortFn)
	sort.Slice(given, func(i, j int) bool {
		a := given[i]
		b := given[j]
		return cmp.Compare(a, b) < 0
	})
	assert.Equal(t, sorted, given)
}

func idleTime(now int64, ev nearcache.EvictableEntryView) int64 {
	return now - maxInt64(ev.CreationTime, ev.LastAccessTime)
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}
