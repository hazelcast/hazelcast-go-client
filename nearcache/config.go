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

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

const (
	defaultMaxEntryCount            = 10_000
	defaultEvictionPolicy           = EvictionPolicyLRU
	defaultStoreInitialDelaySeconds = 600
	defaultStoreIntervalSeconds     = 600
)

// Config is the Near Cache configuration.
type Config struct {
	invalidateOnChange *bool
	// Name is the name of this Near Cache configuration.
	// If the name is not specified, it is set to "default".
	Name string
	// Eviction is the optional eviction configuration for the Near Cache.
	Eviction EvictionConfig
	// InMemoryFormat specifies how the entry values are stored in the Near Cache.
	// InMemoryFormatBinary stores the values after serializing them.
	// InMemoryFormatObject stores the values in their original form.
	// The default is InMemoryFormatBinary.
	InMemoryFormat InMemoryFormat
	// SerializeKeys specifies how the entry keys are stored in the Near Cache.
	// If false, keys are stored in their original form.
	// If true, keys are stored after serializing them.
	// Storing keys in serialized form is required when the key is not hashable, such as slices.
	// The default is false.
	SerializeKeys bool
	// TimeToLiveSeconds is the maximum number of seconds for each entry to stay in the Near Cache (time to live).
	// Entries that are older than TimeToLiveSeconds will automatically be evicted from the Near Cache.
	// Must be non-negative.
	// The value 0 means math.MaxInt32
	// The default is 0.
	TimeToLiveSeconds int
	// MaxIdleSeconds is the maximum number of seconds each entry can stay in the Near Cache as untouched (not-read).
	// Entries that are not read (touched) more than MaxIdleSeconds value will get removed from the Near Cache.
	// Accepts any integer between {@code 0} and {@link Integer#MAX_VALUE}.
	// Must be non-negative.
	// The value 0 means math.MaxInt32
	// The default is 0.
	MaxIdleSeconds int
}

// Clone returns a copy of the configuration.
func (c Config) Clone() Config {
	return Config{
		invalidateOnChange: c.invalidateOnChange,
		Name:               c.Name,
		Eviction:           c.Eviction.Clone(),
		InMemoryFormat:     c.InMemoryFormat,
		SerializeKeys:      c.SerializeKeys,
		TimeToLiveSeconds:  c.TimeToLiveSeconds,
		MaxIdleSeconds:     c.MaxIdleSeconds,
	}
}

// Validate validates the configuration and replaces missing configuration with defaults.
func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "default"
	}
	if c.TimeToLiveSeconds == 0 {
		c.TimeToLiveSeconds = math.MaxInt32
	}
	if c.MaxIdleSeconds == 0 {
		c.MaxIdleSeconds = math.MaxInt32
	}
	if err := c.Eviction.Validate(); err != nil {
		return err
	}
	if err := check.NonNegativeInt32Config(c.TimeToLiveSeconds); err != nil {
		return fmt.Errorf("nearcache.Config: TimeToLiveSeconds: %w", err)
	}
	if err := check.NonNegativeInt32Config(c.MaxIdleSeconds); err != nil {
		return fmt.Errorf("nearcache.Config: MaxIdleSeconds: %w", err)
	}
	if c.InMemoryFormat != InMemoryFormatBinary && c.InMemoryFormat != InMemoryFormatObject {
		return ihzerrors.NewInvalidConfigurationError("nearcache.Config: InMemoryFormat: invalid memory format", nil)
	}
	return nil
}

/*
SetInvalidateOnChange sets if Near Cache entries are invalidated when the entries in the backing data structure are changed (updated or removed).
When this setting is enabled, a client with a Near Cache listens for cluster-wide changes on the entries of the backing data structure.
And the client invalidates its corresponding Near Cache entries.
Changes done on the client always invalidate the Near Cache immediately.
Invalidate on change is true by default.
*/
func (c *Config) SetInvalidateOnChange(enabled bool) {
	c.invalidateOnChange = &enabled
}

// InvalidateOnChange returns true when invalidate on change is enabled.
// See the documentation for SetInvalidateOnChange.
func (c Config) InvalidateOnChange() bool {
	if c.invalidateOnChange == nil {
		return true
	}
	return *c.invalidateOnChange
}

/*
EvictionConfig is the configuration for eviction.

You can set a limit for number of entries.
The default values of the eviction configuration are:

	* EvictionPolicyLRU as eviction policy
	* 10_000 as maximum size for Map.

Eviction policy and comparator are mutually exclusive.
*/
type EvictionConfig struct {
	evictionPolicy *EvictionPolicy
	size           *int32
	comparator     EvictionPolicyComparator
	err            error
}

// Clone returns a copy of the configuration.
func (c EvictionConfig) Clone() EvictionConfig {
	return c
}

// Validate validates the configuration and sets the defaults.
func (c *EvictionConfig) Validate() error {
	if c.err != nil {
		return c.err
	}
	if c.evictionPolicy != nil && c.comparator != nil {
		return ihzerrors.NewInvalidConfigurationError("nearcache.Eviction: only one of EvictionPolicy or Comparator can be configured", nil)
	}
	return nil
}

// SetEvictionPolicy sets the eviction policy of this eviction configuration.
// The default policy is EvictionPolicyLRU which evicts the least recently used entries.
func (c *EvictionConfig) SetEvictionPolicy(policy EvictionPolicy) {
	if policy < 0 || policy >= evictionPolicyCount {
		c.err = ihzerrors.NewInvalidConfigurationError("nearcache.Eviction.SetEvictionPolicy: invalid policy", nil)
		return
	}
	c.evictionPolicy = &policy
}

// EvictionPolicy returns the eviction policy of this eviction configuration.
// See the documentation for SetEvictionPolicy.
func (c EvictionConfig) EvictionPolicy() EvictionPolicy {
	if c.evictionPolicy == nil {
		return defaultEvictionPolicy
	}
	return *c.evictionPolicy
}

// SetSize sets the number of maximum entries before an eviction occurs.
// Accepts any non-negative number.
// The default value is 10_000.
func (c *EvictionConfig) SetSize(size int) {
	s, err := check.NonNegativeInt32(size)
	if err != nil {
		c.err = ihzerrors.NewInvalidConfigurationError("nearcache.Eviction.SetSize: out of range", err)
		return
	}
	c.size = &s
}

// Size returns the number of maximum entries before an eviction occurs.
// See the documentation for SetSize.
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
