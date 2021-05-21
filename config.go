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
	ClientName          string
	LoggerConfig        logger.Config
	SerializationConfig serialization.Config
	ClusterConfig       cluster.Config
}

func NewConfig() Config {
	config := Config{
		ClusterConfig:       cluster.NewConfig(),
		SerializationConfig: serialization.NewConfig(),
		LoggerConfig:        logger.NewConfig(),
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

func (c Config) Clone() Config {
	return Config{
		ClientName:          c.ClientName,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
		LoggerConfig:        c.LoggerConfig.Clone(),
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
	return nil
}
