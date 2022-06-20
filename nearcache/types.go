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

import (
	"fmt"
	"math"
	"time"
)

// InMemoryFormat specifies how the entry values are stored in the Near Cache.
type InMemoryFormat int8

const (
	// InMemoryFormatBinary stores the values after serializing them.
	InMemoryFormatBinary InMemoryFormat = iota
	// InMemoryFormatObject stores the values in their original form.
	InMemoryFormatObject
)

// EvictionPolicy specifies which entry is evicted.
type EvictionPolicy int32

const (
	// EvictionPolicyLRU removes the least recently used entry.
	EvictionPolicyLRU EvictionPolicy = iota
	// EvictionPolicyLFU removes the least frequently used entry.
	EvictionPolicyLFU
	// EvictionPolicyNone removes no entries.
	EvictionPolicyNone
	// EvictionPolicyRandom removes a random entry.
	EvictionPolicyRandom
	evictionPolicyCount // internal
)

// String returns a string representation of the eviction policy.
func (p EvictionPolicy) String() string {
	switch p {
	case EvictionPolicyLRU:
		return "LRU"
	case EvictionPolicyLFU:
		return "LFU"
	case EvictionPolicyNone:
		return "NONE"
	case EvictionPolicyRandom:
		return "RANDOM"
	}
	panic(fmt.Errorf("unknown eviction policy: %d", p))
}

// Stats contains statistics for a Near Cache instance.
type Stats struct {
	// Evictions is the number of evictions.
	Evictions int64
	// Evictions is the number of expirations.
	Expirations int64
	// Hits is the number of times a key was found in the Near Cache.
	Hits int64
	// InvalidationRequests is the number of invalidation requests.
	// An invalidation request may be successful or not.
	InvalidationRequests int64
	// Invalidations is the number of successful invalidations.
	Invalidations int64
	// Misses is the number of times a key was not found in the Near Cache.
	Misses int64
	// OwnedEntryCount is the number of entries in the Near Cache.
	OwnedEntryCount int64
	// OwnedEntryMemoryCost is the estimated memory cost of the entries in the Near Cache.
	OwnedEntryMemoryCost int64
	// LastPersistenceKeyCount is the number of keys saved in the last persistence task.
	LastPersistenceKeyCount int64
	// LastPersistenceWrittenBytes is the size of the last persistence task.
	LastPersistenceWrittenBytes int64
	// PersistenceCount is the number of completed persistence tasks.
	PersistenceCount int64
	// LastPersistenceTime is the time of the last completed persistence task.
	LastPersistenceTime time.Time
	// LastPersistenceDuration is the duration of the last completed persistence task.
	LastPersistenceDuration time.Duration
	// LastPersistenceFailure is the error message of the last completed persistence task.
	LastPersistenceFailure string
	// CreationTime is the time the Near Cache was initialized.
	CreationTime time.Time
}

// Ratio returns the ratio of hits to misses.
// Returns math.Nan if there were no hits or misses.
// Otherwise returns +math.Inf if there were no misses.
func (s Stats) Ratio() float64 {
	if s.Misses == 0 {
		if s.Hits == 0 {
			return math.NaN()
		}
		return math.Inf(1)
	}
	return (float64(s.Hits) / float64(s.Misses)) * 100.0
}

// EvictionPolicyComparator is used for comparing entries to be evicted.
type EvictionPolicyComparator interface {
	// Compare returns a negative integer if a is less than b, 0 if a is equal to b or a positive integer if a is greater than b.
	Compare(a, b EvictableEntryView) int
}

// EvictableEntryView is the contract point from the end user perspective for serving/accessing entries that can be evicted.
type EvictableEntryView interface {
	// Hits is the number of accesses to the entry.
	Hits() int64
	// Key is the key of the entry.
	Key() interface{}
	// Value is the value of the entry.
	Value() interface{}
	// CreationTime is the creation time of the entry in milliseconds.
	CreationTime() int64
	// LastAccessTime is the time when the entry was last accessed in milliseconds.
	LastAccessTime() int64
}
