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

package nearcache

import "github.com/hazelcast/hazelcast-go-client/nearcache"

type simpleEvictionPolicyComparator struct {
	f func(e1, e2 nearcache.EvictableEntryView) int
}

func (pc simpleEvictionPolicyComparator) Compare(e1, e2 nearcache.EvictableEntryView) int {
	return pc.f(e1, e2)
}

var LRUEvictionPolicyComparator = simpleEvictionPolicyComparator{
	f: func(e1, e2 nearcache.EvictableEntryView) int {
		time1 := maxInt64(e1.CreationTime, e1.LastAccessTime)
		time2 := maxInt64(e2.CreationTime, e2.LastAccessTime)
		if time1 < time2 {
			return -1
		}
		if time1 > time2 {
			return 1
		}
		return 0
	},
}

var LFUEvictionPolicyComparator = simpleEvictionPolicyComparator{
	f: func(e1, e2 nearcache.EvictableEntryView) int {
		// assuming both e1.Hits and e2.Hits are positive
		result := e1.Hits - e2.Hits
		if result == 0 {
			// if hits are same, we try to select oldest entry to evict
			result = e1.CreationTime - e2.CreationTime
		}
		if result < 0 {
			return -1
		}
		if result > 0 {
			return 1
		}
		return 0
	},
}

var RandomEvictionPolicyComparator = simpleEvictionPolicyComparator{
	f: func(e1, e2 nearcache.EvictableEntryView) int {
		return 0
	},
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}
