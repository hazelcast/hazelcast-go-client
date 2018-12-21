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

package config

import "time"

type InMemoryFormat int32
type EvictionPolicy int32

const (
	InMemoryFormatBinary InMemoryFormat = iota
	InMemoryFormatObject
)

const (
	EvictionPolicyNone EvictionPolicy = iota
	EvictionPolicyLru
	EvictionPolicyLfu
)

const (
	defaultEvictionPolicy           = EvictionPolicyLru
	defaultMaxEntryCount      int32 = 10000
	defaultMemoryFormat             = InMemoryFormatBinary
	defaultInvalidateOnChange       = true
)

type NearCacheConfig struct {
	inMemoryFormat     InMemoryFormat
	evictionPolicy     EvictionPolicy
	maxEntryCount      int32
	maxIdleDuration    time.Duration
	timeToLive         time.Duration
	invalidateOnChange bool
}

func NewNearCacheConfig() *NearCacheConfig {
	return &NearCacheConfig{
		inMemoryFormat:     defaultMemoryFormat,
		evictionPolicy:     defaultEvictionPolicy,
		maxEntryCount:      defaultMaxEntryCount,
		invalidateOnChange: defaultInvalidateOnChange,
	}
}

func (n *NearCacheConfig) SetMaxEntryCount(size int32) {
	n.maxEntryCount = size
}

func (n *NearCacheConfig) MaxEntryCount() int32 {
	return n.maxEntryCount
}

func (n *NearCacheConfig) SetEvictionPolicy(policy EvictionPolicy) {
	n.evictionPolicy = policy
}

func (n *NearCacheConfig) EvictionPolicy() EvictionPolicy {
	return n.evictionPolicy
}

func (n *NearCacheConfig) IsSerializeKeys() bool {
	return false
}

func (n *NearCacheConfig) InvalidateOnChange() bool {
	return n.invalidateOnChange
}

func (n *NearCacheConfig) SetInvalidateOnChange(invalidateOnChange bool) {
	n.invalidateOnChange = invalidateOnChange
}

func (n *NearCacheConfig) InMemoryFormat() InMemoryFormat {
	return n.inMemoryFormat
}

func (n *NearCacheConfig) SetMaxIdleDuration(maxIdleDuration time.Duration) {
	n.maxIdleDuration = maxIdleDuration
}

func (n *NearCacheConfig) MaxIdleDuration() time.Duration {
	return n.maxIdleDuration
}

func (n *NearCacheConfig) TimeToLive() time.Duration {
	return n.timeToLive
}
