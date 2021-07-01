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
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Config contains configuration for a client.
// Prefer to create the configuration using the NewConfig function.
type Config struct {
	lifecycleListeners  map[types.UUID]LifecycleStateChangeHandler
	membershipListeners map[types.UUID]cluster.MembershipStateChangeHandler
	Labels              []string `json:",omitempty"`
	ClientName          string   `json:",omitempty"`
	LoggerConfig        logger.Config
	SerializationConfig serialization.Config
	ClusterConfig       cluster.Config
	StatsConfig         StatsConfig
}

func NewConfig() Config {
	config := Config{
		ClusterConfig:       cluster.NewConfig(),
		SerializationConfig: serialization.NewConfig(),
		LoggerConfig:        logger.NewConfig(),
		StatsConfig:         newStatsConfig(),
		lifecycleListeners:  map[types.UUID]LifecycleStateChangeHandler{},
		membershipListeners: map[types.UUID]cluster.MembershipStateChangeHandler{},
	}
	return config
}

// AddLifecycleListener adds a lifecycle listener.
// The listener is attached to the client before the client starts, so all lifecycle events can be received.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Config) AddLifecycleListener(handler LifecycleStateChangeHandler) types.UUID {
	if c.lifecycleListeners == nil {
		c.lifecycleListeners = map[types.UUID]LifecycleStateChangeHandler{}
	}
	id := types.NewUUID()
	c.lifecycleListeners[id] = handler
	return id
}

// AddMembershipListener adds a membership listeener.
// The listener is attached to the client before the client starts, so all membership events can be received.
// Use the returned subscription ID to remove the listener.
func (c *Config) AddMembershipListener(handler cluster.MembershipStateChangeHandler) types.UUID {
	if c.membershipListeners == nil {
		c.membershipListeners = map[types.UUID]cluster.MembershipStateChangeHandler{}
	}
	id := types.NewUUID()
	c.membershipListeners[id] = handler
	return id
}

// SetLabels sets the labels for the client.
// These labels are displayed in the Hazelcast Management Center.
func (c *Config) SetLabels(labels ...string) {
	c.Labels = labels
}

func (c Config) Clone() Config {
	newLabels := make([]string, len(c.Labels))
	copy(newLabels, c.Labels)
	return Config{
		ClientName:          c.ClientName,
		Labels:              newLabels,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
		LoggerConfig:        c.LoggerConfig.Clone(),
		StatsConfig:         c.StatsConfig.clone(),
		// both lifecycleListeners and membershipListeners are not used verbatim in client creator
		// so no need to copy them
		lifecycleListeners:  c.lifecycleListeners,
		membershipListeners: c.membershipListeners,
	}
}

func (c Config) Validate() error {
	if err := c.ClusterConfig.Validate(); err != nil {
		return err
	}
	if err := c.SerializationConfig.Validate(); err != nil {
		return err
	}
	if err := c.LoggerConfig.Validate(); err != nil {
		return err
	}
	if err := c.StatsConfig.Validate(); err != nil {
		return err
	}
	return nil
}

// StatsConfig contains configuration for Management Center.
type StatsConfig struct {
	// Enabled enables collecting statistics.
	Enabled bool
	// Period is the period of statistics collection.
	Period time.Duration
}

func newStatsConfig() StatsConfig {
	return StatsConfig{
		Enabled: false,
		Period:  3 * time.Second,
	}
}

func (c StatsConfig) clone() StatsConfig {
	return c
}

func (c StatsConfig) Validate() error {
	if c.Enabled && c.Period <= 0 {
		return hzerrors.ErrConfigInvalidStatsPeriod
	}
	return nil
}
