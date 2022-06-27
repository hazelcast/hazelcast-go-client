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
	"fmt"
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Config contains configuration for a client.
// Zero value of Config is the default configuration.
type Config struct {
	lifecycleListeners  map[types.UUID]LifecycleStateChangeHandler
	membershipListeners map[types.UUID]cluster.MembershipStateChangeHandler
	Nearcaches          map[string]nearcache.Config       `json:",omitempty"`
	FlakeIDGenerators   map[string]FlakeIDGeneratorConfig `json:",omitempty"`
	Labels              []string                          `json:",omitempty"`
	ClientName          string                            `json:",omitempty"`
	Logger              logger.Config                     `json:",omitempty"`
	Failover            cluster.FailoverConfig            `json:",omitempty"`
	Serialization       serialization.Config              `json:",omitempty"`
	Cluster             cluster.Config                    `json:",omitempty"`
	Stats               StatsConfig                       `json:",omitempty"`
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

// AddNearCache adds a near cache configuration.
func (c *Config) AddNearCache(cfg nearcache.Config) {
	c.ensureNearCacheConfigs()
	c.Nearcaches[cfg.Name] = cfg
}

// GetNearCacheConfig returns the first configuration that matches the given pattern.
// Returns hzerrors.ErrInvalidConfiguration if the pattern matches more than one configuration.
func (c *Config) GetNearCacheConfig(pattern string) (nearcache.Config, bool, error) {
	c.ensureNearCacheConfigs()
	nc, ok, err := c.lookupNearCacheByPattern(pattern)
	if err != nil {
		return nc, false, err
	}
	if ok {
		return nc, true, nil
	}
	// config not found, return the default if it exists
	nc, ok = c.Nearcaches["default"]
	return nc, ok, nil
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
	newFlakeIDConfigs := make(map[string]FlakeIDGeneratorConfig, len(c.FlakeIDGenerators))
	for k, v := range c.FlakeIDGenerators {
		newFlakeIDConfigs[k] = v
	}
	nccs := c.copyNearCacheConfig()
	return Config{
		ClientName:        c.ClientName,
		Labels:            newLabels,
		FlakeIDGenerators: newFlakeIDConfigs,
		Nearcaches:        nccs,
		Cluster:           c.Cluster.Clone(),
		Failover:          c.Failover.Clone(),
		Serialization:     c.Serialization.Clone(),
		Logger:            c.Logger.Clone(),
		Stats:             c.Stats.clone(),
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
	c.ensureFlakeIDGenerators()
	for _, v := range c.FlakeIDGenerators {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	c.Nearcaches = c.copyNearCacheConfig()
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

func (c *Config) ensureFlakeIDGenerators() {
	if c.FlakeIDGenerators == nil {
		c.FlakeIDGenerators = map[string]FlakeIDGeneratorConfig{}
	}
}

func (c *Config) ensureNearCacheConfigs() {
	if c.Nearcaches == nil {
		c.Nearcaches = map[string]nearcache.Config{}
	}
}

func (c *Config) lookupNearCacheByPattern(itemName string) (nearcache.Config, bool, error) {
	if candidate, ok := c.Nearcaches[itemName]; ok {
		return candidate, true, nil
	}
	key, err := matchingPointMatches(c.Nearcaches, itemName)
	if err != nil {
		return nearcache.Config{}, false, err
	}
	if key == "" {
		// not found
		return nearcache.Config{}, false, nil
	}
	return c.Nearcaches[key], true, nil
}

// AddFlakeIDGenerator validates the values and adds new FlakeIDGeneratorConfig with the given name.
func (c *Config) AddFlakeIDGenerator(name string, prefetchCount int32, prefetchExpiry types.Duration) error {
	if _, ok := c.FlakeIDGenerators[name]; ok {
		return hzerrors.NewIllegalArgumentError(fmt.Sprintf("config already exists for %s", name), nil)
	}
	idConfig := FlakeIDGeneratorConfig{PrefetchCount: prefetchCount, PrefetchExpiry: prefetchExpiry}
	if err := idConfig.Validate(); err != nil {
		return err
	}
	c.ensureFlakeIDGenerators()
	c.FlakeIDGenerators[name] = idConfig
	return nil
}

func (c Config) copyNearCacheConfig() map[string]nearcache.Config {
	c.ensureNearCacheConfigs()
	configs := make(map[string]nearcache.Config, len(c.Nearcaches))
	for k, v := range c.Nearcaches {
		configs[k] = v.Clone()
	}
	return configs
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
	if err := check.EnsureNonNegativeDuration((*time.Duration)(&c.Period), 5*time.Second, "invalid period"); err != nil {
		return err
	}
	return nil
}

const (
	maxFlakeIDPrefetchCount      = 100_000
	defaultFlakeIDPrefetchCount  = 100
	defaultFlakeIDPrefetchExpiry = types.Duration(10 * time.Minute)
)

// FlakeIDGeneratorConfig contains configuration for the pre-fetching behavior of FlakeIDGenerator.
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
	} else if err := check.WithinRangeInt32(f.PrefetchCount, 1, maxFlakeIDPrefetchCount); err != nil {
		return err
	}
	if err := check.EnsureNonNegativeDuration((*time.Duration)(&f.PrefetchExpiry), time.Duration(defaultFlakeIDPrefetchExpiry), "invalid duration"); err != nil {
		return err
	}
	return nil
}

func matchingPointMatches(patterns map[string]nearcache.Config, itemName string) (string, error) {
	// port of: com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher#matches
	var candidate, duplicate string
	var hasDup bool
	last := -1
	for p := range patterns {
		mp := getMatchingPoint(p, itemName)
		if mp > -1 && mp >= last {
			hasDup = mp == last
			if hasDup {
				duplicate = candidate
			}
			last = mp
			candidate = p
		}
	}
	if hasDup {
		msg := fmt.Sprintf(`ambiguous configuration for item: "%s": "%s" vs "%s"`, itemName, candidate, duplicate)
		return "", hzerrors.NewInvalidConfigurationError(msg, nil)
	}
	return candidate, nil
}

func getMatchingPoint(pattern, itemName string) int {
	// port of: com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher#getMatchingPoint
	index := strings.Index(pattern, "*")
	if index == -1 {
		return -1
	}
	first := pattern[:index]
	if !strings.HasPrefix(itemName, first) {
		return -1
	}
	second := pattern[index+1:]
	if !strings.HasSuffix(itemName, second) {
		return -1
	}
	return len(first) + len(second)
}
