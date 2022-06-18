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
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	defaultMaxEntryCount            = 10_000
	defaultEvictionPolicy           = EvictionPolicyLRU
	defaultStoreInitialDelaySeconds = 600
	defaultStoreIntervalSeconds     = 600
	defaultMemoryFormat             = InMemoryFormatBinary
)

type Config struct {
	invalidateOnChange *bool
	Name               string
	EvictionConfig     EvictionConfig
	PreloaderConfig    PreloaderConfig
	InMemoryFormat     InMemoryFormat
	SerializeKeys      bool
	TimeToLiveSeconds  int32
	MaxIdleSeconds     int32
}

func (c Config) Clone() Config {
	return c
}

func (c *Config) Validate() error {
	if err := c.EvictionConfig.Validate(); err != nil {
		return err
	}
	if err := c.PreloaderConfig.Validate(); err != nil {
		return err
	}
	if c.Name == "" {
		c.Name = "default"
	}
	return nil
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

type EvictionPolicyComparator interface {
	Compare(a, b types.EvictableEntryView) int
}

/*
EvictionConfig is the configuration for eviction.

You can set a limit for number of entries or total memory cost of entries.
The default values of the eviction configuration are:

	* EvictionPolicyLRU as eviction policy
	* MaxSizePolicyEntryCount as max size policy
	* 2147483647 as maximum size for on-heap Map
	* 10_000 as maximum size for all other data structures and configurations
*/
type EvictionConfig struct {
	evictionPolicy *EvictionPolicy
	size           *int32
	comparator     EvictionPolicyComparator
}

func (c EvictionConfig) Clone() EvictionConfig {
	return c
}

// Validate validates the configuration and sets the defaults.
func (c *EvictionConfig) Validate() error {
	if c.evictionPolicy != nil && c.comparator != nil {
		return ihzerrors.NewInvalidConfigurationError("only one of EvictionPolicy or Comparator can be configured", nil)
	}
	return nil
}

// SetEvictionPolicy sets the eviction policy of this eviction configuration.
func (c *EvictionConfig) SetEvictionPolicy(policy EvictionPolicy) {
	c.evictionPolicy = &policy
}

// EvictionPolicy returns the eviction policy of this eviction configuration.
func (c EvictionConfig) EvictionPolicy() EvictionPolicy {
	if c.evictionPolicy == nil {
		return defaultEvictionPolicy
	}
	return *c.evictionPolicy
}

// SetSize sets the size the size which is used by the MaxSizePolicy.
// The interpretation of the value depends on the configured MaxSizePolicy.
// Accepts any non-negative number.
// The default value is 10_000.
func (c *EvictionConfig) SetSize(size int) error {
	s, err := check.NonNegativeInt32(size)
	if err != nil {
		return err
	}
	c.size = &s
	return nil
}

// Size returns the size the size which is used by the MaxSizePolicy.
func (c EvictionConfig) Size() int {
	if c.size == nil {
		return defaultMaxEntryCount
	}
	return int(*c.size)
}

// SetComparator sets the eviction policy comparator.
func (c *EvictionConfig) SetComparator(cmp EvictionPolicyComparator) {
	c.comparator = cmp
}

// Comparator returns the eviction policy comparator.
func (c EvictionConfig) Comparator() EvictionPolicyComparator {
	return c.comparator
}

type PreloaderConfig struct {
	Directory                string
	StoreInitialDelaySeconds int32
	StoreIntervalSeconds     int32
	Enabled                  bool
}

func (c *PreloaderConfig) Validate() error {
	return nil
}
