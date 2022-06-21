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

type simpleEntry struct {
	creationTime   int64
	lastAccessTime int64
	hits           int64
}

func (s simpleEntry) Hits() int64 {
	return s.hits
}

func (s simpleEntry) Key() interface{} {
	return nil
}

func (s simpleEntry) Value() interface{} {
	return nil
}

func (s simpleEntry) CreationTime() int64 {
	return s.creationTime
}

func (s simpleEntry) LastAccessTime() int64 {
	return s.lastAccessTime
}

func TestLRUComparatorDoesNotPrematurelySelectNewlyCreatedEntries(t *testing.T) {
	// 1. Create expected list of ordered elements by
	// sorting entries based on their idle-times. Longest
	// idle time must be the first element of the list.
	// 2. Then sort given entries by using LRU eviction comparator.
	const now = 20
	given := []nearcache.EvictableEntryView{
		simpleEntry{creationTime: 1, lastAccessTime: 0},
		simpleEntry{creationTime: 2, lastAccessTime: 3},
		simpleEntry{creationTime: 2, lastAccessTime: 0},
		simpleEntry{creationTime: 4, lastAccessTime: 4},
		simpleEntry{creationTime: 5, lastAccessTime: 20},
		simpleEntry{creationTime: 6, lastAccessTime: 6},
		simpleEntry{creationTime: 7, lastAccessTime: 0},
		simpleEntry{creationTime: 9, lastAccessTime: 15},
		simpleEntry{creationTime: 10, lastAccessTime: 10},
		simpleEntry{creationTime: 10, lastAccessTime: 0},
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
		simpleEntry{hits: 5, creationTime: 2},
		simpleEntry{hits: 10, creationTime: 5},
		simpleEntry{hits: 2, creationTime: 7},
		simpleEntry{hits: 5, creationTime: 4},
	}
	sortFn := func(sorted []nearcache.EvictableEntryView) func(i, j int) bool {
		return func(i, j int) bool {
			a := sorted[i]
			b := sorted[j]
			if a.Hits() < b.Hits() {
				return true
			}
			if a.Hits() > b.Hits() {
				return false
			}
			return a.CreationTime() < b.CreationTime()
		}
	}
	evictionPolicyHelper(t, given, sortFn, inearcache.LFUEvictionPolicyComparator)
}

func TestRandomEvictionPolicyComparator(t *testing.T) {
	given := []nearcache.EvictableEntryView{
		simpleEntry{hits: 5},
		simpleEntry{hits: 10},
		simpleEntry{hits: 2},
		simpleEntry{hits: 7},
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
	return now - maxInt64(ev.CreationTime(), ev.LastAccessTime())
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}
