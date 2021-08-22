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

package hazelcast

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	validate "github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Config contains configuration for a client.
// Zero value of Config is the default configuration.
type Config struct {
	lifecycleListeners      map[types.UUID]LifecycleStateChangeHandler
	membershipListeners     map[types.UUID]cluster.MembershipStateChangeHandler
	FlakeIDGeneratorConfigs map[string]FlakeIDGeneratorConfig `json:",omitempty"`
	Labels                  []string                          `json:",omitempty"`
	ClientName              string                            `json:",omitempty"`
	Logger                  logger.Config                     `json:",omitempty"`
	Failover                cluster.FailoverConfig            `json:",omitempty"`
	Serialization           serialization.Config              `json:",omitempty"`
	Cluster                 cluster.Config                    `json:",omitempty"`
	Stats                   StatsConfig                       `json:",omitempty"`
}

// NewConfig creates the default configuration.
func NewConfig() Config {
	return Config{}
}

// AddLifecycleListener adds a lifecycle listener.
// The listener is attached to the client before the client starts, so all lifecycle events can be received.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Config) AddLifecycleListener(handler LifecycleStateChangeHandler) types.UUID {
	c.ensureLifecycleListeners()
	id := types.NewUUID()
	c.lifecycleListeners[id] = handler
	return id
}

// AddMembershipListener adds a membership listener.
// The listener is attached to the client before the client starts, so all membership events can be received.
// Use the returned subscription ID to remove the listener.
func (c *Config) AddMembershipListener(handler cluster.MembershipStateChangeHandler) types.UUID {
	c.ensureMembershipListeners()
	id := types.NewUUID()
	c.membershipListeners[id] = handler
	return id
}

// SetLabels sets the labels for the client.
// These labels are displayed in the Hazelcast Management Center.
func (c *Config) SetLabels(labels ...string) {
	c.Labels = labels
}

// Clone returns a copy of the configuration.
func (c *Config) Clone() Config {
	c.ensureLifecycleListeners()
	c.ensureMembershipListeners()
	newLabels := make([]string, len(c.Labels))
	copy(newLabels, c.Labels)
	var newFlakeIDConfigs map[string]FlakeIDGeneratorConfig
	if c.FlakeIDGeneratorConfigs != nil {
		newFlakeIDConfigs = make(map[string]FlakeIDGeneratorConfig, len(c.FlakeIDGeneratorConfigs))
		for k, v := range c.FlakeIDGeneratorConfigs {
			newFlakeIDConfigs[k] = v
		}
	}
	return Config{
		ClientName:              c.ClientName,
		Labels:                  newLabels,
		FlakeIDGeneratorConfigs: newFlakeIDConfigs,
		Cluster:                 c.Cluster.Clone(),
		Failover:                c.Failover.Clone(),
		Serialization:           c.Serialization.Clone(),
		Logger:                  c.Logger.Clone(),
		Stats:                   c.Stats.clone(),
		// both lifecycleListeners and membershipListeners are not used verbatim in client creator
		// so no need to copy them
		lifecycleListeners:  c.lifecycleListeners,
		membershipListeners: c.membershipListeners,
	}
}

// Validate validates the configuration and replaces missing configuration with defaults.
func (c *Config) Validate() error {
	if err := c.Cluster.Validate(); err != nil {
		return err
	}
	if err := c.Failover.Validate(c.Cluster); err != nil {
		return err
	}
	if err := c.Serialization.Validate(); err != nil {
		return err
	}
	if err := c.Logger.Validate(); err != nil {
		return err
	}
	if err := c.Stats.Validate(); err != nil {
		return err
	}
	for _, v := range c.FlakeIDGeneratorConfigs {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) ensureLifecycleListeners() {
	if c.lifecycleListeners == nil {
		c.lifecycleListeners = map[types.UUID]LifecycleStateChangeHandler{}
	}
}

func (c *Config) ensureMembershipListeners() {
	if c.membershipListeners == nil {
		c.membershipListeners = map[types.UUID]cluster.MembershipStateChangeHandler{}
	}
}

func (c *Config) getFlakeIDGeneratorConfig(name string) FlakeIDGeneratorConfig {
	if conf, ok := c.FlakeIDGeneratorConfigs[name]; ok {
		return conf
	}
	return FlakeIDGeneratorConfig{
		PrefetchCount:  defaultFlakeIDPrefetchCount,
		PrefetchExpiry: defaultFlakeIDPrefetchExpiry,
	}
}

// StatsConfig contains configuration for Management Center.
type StatsConfig struct {
	// Enabled enables collecting statistics.
	Enabled bool `json:",omitempty"`
	// Period is the period of statistics collection.
	Period types.Duration `json:",omitempty"`
}

func (c StatsConfig) clone() StatsConfig {
	return c
}

// Validate validates the stats configuration and replaces missing configuration with defaults.
func (c *StatsConfig) Validate() error {
	if err := validate.NonNegativeDuration(&c.Period, 5*time.Second, "invalid period"); err != nil {
		return err
	}
	return nil
}

const (
	maxFlakeIDPrefetchCount      = 100_000
	defaultFlakeIDPrefetchCount  = 100
	defaultFlakeIDPrefetchExpiry = types.Duration(10 * time.Minute)
)

// FlakeIDGeneratorConfig configures pre-fetching behavior for FlakeIDGenerator.
type FlakeIDGeneratorConfig struct {
	// PrefetchCount defines the number of pre-fetched IDs from cluster.
	// The allowed range is [1, 100_000] and defaults to 100.
	PrefetchCount int32 `json:",omitempty"`
	// PrefetchExpiry defines the expiry duration of pre-fetched IDs. Defaults to 10 minutes.
	PrefetchExpiry types.Duration `json:",omitempty"`
}

func (f *FlakeIDGeneratorConfig) Validate() error {
	if f.PrefetchCount == 0 {
		f.PrefetchCount = defaultFlakeIDPrefetchCount
	} else if err := validate.IsWithinInclusiveRangeInt32(f.PrefetchCount, 1, maxFlakeIDPrefetchCount); err != nil {
		return err
	}
	if err := validate.NonNegativeDuration(&f.PrefetchExpiry, time.Duration(defaultFlakeIDPrefetchExpiry), "invalid duration"); err != nil {
		return err
	}
	return nil
}
