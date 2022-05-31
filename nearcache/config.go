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

import "github.com/hazelcast/hazelcast-go-client/internal/check"

const (
	DefaultMaxEntryCount            = 10_000
	DefaultMaxSizePolicy            = MaxSizePolicyEntryCount
	DefaultEvictionPolicy           = EvictionPolicyLRU
	DefaultStoreInitialDelaySeconds = 600
	DefaultStoreIntervalSeconds     = 600
)

type Config struct {
	invalidateOnChange *bool
	Name               string
	EvictionConfig     EvictionConfig
	InMemoryFormat     InMemoryFormat
	SerializeKeys      bool
	LocalUpdatePolicy  LocalUpdatePolicy
	TimeToLiveSeconds  int32
	MaxIdleSeconds     int32
	CacheLocalEntries  bool
	PreloaderConfig    PreloaderConfig
}

func (c *Config) SetInvalidateOnChange(enabled bool) {
	c.invalidateOnChange = &enabled
}

func (c Config) InvalidateOnChange() bool {
	if c.invalidateOnChange == nil {
		return true
	}
	return *c.invalidateOnChange
}

type EvictionConfig struct {
	maxSizePolicy  *MaxSizePolicy
	evictionPolicy *EvictionPolicy
	size           *int32
}

func (c *EvictionConfig) Validate() error {
	return nil
}

func (c *EvictionConfig) SetMaxSizePolicy(policy MaxSizePolicy) {
	c.maxSizePolicy = &policy
}

func (c EvictionConfig) MaxSizePolicy() MaxSizePolicy {
	if c.maxSizePolicy == nil {
		return DefaultMaxSizePolicy
	}
	return *c.maxSizePolicy
}

func (c *EvictionConfig) SetEvictionPolicy(policy EvictionPolicy) {
	c.evictionPolicy = &policy
}

func (c EvictionConfig) EvictionPolicy() EvictionPolicy {
	if c.evictionPolicy == nil {
		return DefaultEvictionPolicy
	}
	return *c.evictionPolicy
}

func (c *EvictionConfig) SetSize(size int) error {
	s, err := check.NonNegativeInt32(size)
	if err != nil {
		return err
	}
	c.size = &s
	return nil
}

type PreloaderConfig struct {
	Enabled                  bool
	Directory                string
	StoreInitialDelaySeconds int32
	StoreIntervalSeconds     int32
}
