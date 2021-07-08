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
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Config contains configuration for a client.
// Prefer to create the configuration using the NewConfig function.
type Config struct {
	lifecycleListeners  map[types.UUID]LifecycleStateChangeHandler
	membershipListeners map[types.UUID]cluster.MembershipStateChangeHandler
	Labels              []string             `json:",omitempty"`
	ClientName          string               `json:",omitempty"`
	Logger              logger.Config        `json:",omitempty"`
	Serialization       serialization.Config `json:",omitempty"`
	Cluster             cluster.Config       `json:",omitempty"`
	Stats               StatsConfig          `json:",omitempty"`
}

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

// AddMembershipListener adds a membership listeener.
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

func (c *Config) Clone() Config {
	c.ensureLifecycleListeners()
	c.ensureMembershipListeners()
	newLabels := make([]string, len(c.Labels))
	copy(newLabels, c.Labels)
	return Config{
		ClientName:    c.ClientName,
		Labels:        newLabels,
		Cluster:       c.Cluster.Clone(),
		Serialization: c.Serialization.Clone(),
		Logger:        c.Logger.Clone(),
		Stats:         c.Stats.clone(),
		// both lifecycleListeners and membershipListeners are not used verbatim in client creator
		// so no need to copy them
		lifecycleListeners:  c.lifecycleListeners,
		membershipListeners: c.membershipListeners,
	}
}

func (c *Config) Validate() error {
	if err := c.Cluster.Validate(); err != nil {
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

func (c *StatsConfig) Validate() error {
	if c.Period <= 0 {
		c.Period = types.Duration(5 * time.Second)
	}
	return nil
}
