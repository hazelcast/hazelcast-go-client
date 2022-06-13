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

import "fmt"

type MaxSizePolicy int32

func (p MaxSizePolicy) String() string {
	switch p {
	case MaxSizePolicyEntryCount:
		return "ENTRY_COUNT"
	}
	panic(fmt.Errorf("unknown max size policy: %d", p))
}

const (
	// MaxSizePolicyEntryCount is the policy based on maximum number of entries stored per data structure (map, etc).
	MaxSizePolicyEntryCount MaxSizePolicy = 6
)

type InMemoryFormat int8

const (
	InMemoryFormatBinary InMemoryFormat = 0
	InMemoryFormatObject InMemoryFormat = 1
)

type EvictionPolicy int32

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

const (
	EvictionPolicyLRU    EvictionPolicy = 0
	EvictionPolicyLFU    EvictionPolicy = 1
	EvictionPolicyNone   EvictionPolicy = 2
	EvictionPolicyRandom EvictionPolicy = 3
)

type Stats struct {
	Hits            int64
	Misses          int64
	Expirations     int64
	OwnedMemoryCost int64
	OwnedEntryCount int64
}
