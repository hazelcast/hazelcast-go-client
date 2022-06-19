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
	InMemoryFormatBinary InMemoryFormat = iota
	InMemoryFormatObject
)

type EvictionPolicy int32

const (
	EvictionPolicyLRU EvictionPolicy = iota
	EvictionPolicyLFU
	EvictionPolicyNone
	EvictionPolicyRandom
	evictionPolicyCount // internal
)

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

type LocalUpdatePolicy int32

const (
	LocalUpdatePolicyInvalidate LocalUpdatePolicy = 0
)
