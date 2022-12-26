/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// InMemoryFormat specifies how the entry values are stored in the Near Cache.
type InMemoryFormat int8

const (
	// InMemoryFormatBinary stores the values after serializing them.
	InMemoryFormatBinary InMemoryFormat = iota
	// InMemoryFormatObject stores the values in their original form.
	InMemoryFormatObject
)

// UnmarshalText unmarshals the in memory format from a byte array.
func (m *InMemoryFormat) UnmarshalText(b []byte) error {
	s := string(b)
	switch strings.ToLower(s) {
	case "binary":
		*m = InMemoryFormatBinary
	case "object":
		*m = InMemoryFormatObject
	default:
		msg := fmt.Sprintf("unknown in memory format: %s", s)
		return hzerrors.NewIllegalArgumentError(msg, nil)
	}
	return nil
}

// MarshalText marshals the text in memory format to a byte array.
func (m InMemoryFormat) MarshalText() ([]byte, error) {
	switch m {
	case InMemoryFormatBinary:
		return []byte("binary"), nil
	case InMemoryFormatObject:
		return []byte("object"), nil
	default:
		err := hzerrors.NewIllegalArgumentError(fmt.Sprintf("unknown in memory format: %d", m), nil)
		return nil, err
	}
}

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

func (p *EvictionPolicy) UnmarshalText(b []byte) error {
	s := string(b)
	switch strings.ToLower(s) {
	case "lru":
		*p = EvictionPolicyLRU
	case "lfu":
		*p = EvictionPolicyLFU
	case "none":
		*p = EvictionPolicyNone
	case "random":
		*p = EvictionPolicyRandom
	default:
		msg := fmt.Sprintf("unknown eviction policy: %s", s)
		return hzerrors.NewIllegalArgumentError(msg, nil)
	}
	return nil
}

func (p EvictionPolicy) MarshalText() ([]byte, error) {
	switch p {
	case EvictionPolicyLRU:
		return []byte("lru"), nil
	case EvictionPolicyLFU:
		return []byte("lfu"), nil
	case EvictionPolicyNone:
		return []byte("none"), nil
	case EvictionPolicyRandom:
		return []byte("random"), nil
	default:
		err := hzerrors.NewIllegalArgumentError(fmt.Sprintf("unknown policy: %d", p), nil)
		return nil, err
	}
}

// Stats contains statistics for a Near Cache instance.
type Stats struct {
	// InvalidationRequests is the number of times an invalidation was requested.
	InvalidationRequests int64
	// Misses is the number of times an entry was not found in the Near Cache.
	Misses int64
	// Hits is the number of times an entry was found in the Near Cache.
	Hits int64
	// Expirations is the number of expirations due to TTL and max idle constraints.
	Expirations int64
	// Evictions is the number of evictions.
	Evictions int64
	// OwnedEntryCount is the number of entries in the Near Cache.
	OwnedEntryCount int64
	// OwnedEntryMemoryCost is the estimated memory cost in bytes for the entries in the Near Cache.
	OwnedEntryMemoryCost int64
	// Invalidations is the number of successful invalidations.
	Invalidations int64
	// LastPersistenceKeyCount is the number of keys saved in the last persistence task when the pre-load feature is enabled.
	LastPersistenceKeyCount int64
	// LastPersistenceWrittenBytes is number of bytes written in the last persistence task when the pre-load feature is enabled.
	LastPersistenceWrittenBytes int64
	// PersistenceCount is the number of completed persistence tasks when the pre-load feature is enabled.
	PersistenceCount int64
	// CreationTime is the time the Near Cache was initialized.
	CreationTime time.Time
	// LastPersistenceTime is the time of the last completed persistence task when the pre-load feature is enabled.
	LastPersistenceTime time.Time
	// LastPersistenceFailure is the error message of the last completed persistence task when the pre-load feature is enabled.
	LastPersistenceFailure string
	// LastPersistenceDuration is the duration of the last completed persistence task when the pre-load feature is enabled.
	LastPersistenceDuration time.Duration
}

// Ratio returns the ratio of hits to misses.
// Returns math.Nan if there were no hits and misses.
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
