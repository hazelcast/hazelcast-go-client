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

package cluster

import (
	"time"

	validate "github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const defaultName = "dev"

type Config struct {
	loadBalancer       LoadBalancer
	Security           SecurityConfig
	Name               string `json:",omitempty"`
	Cloud              CloudConfig
	Network            NetworkConfig
	ConnectionStrategy ConnectionStrategyConfig
	InvocationTimeout  types.Duration `json:",omitempty"`
	HeartbeatInterval  types.Duration `json:",omitempty"`
	HeartbeatTimeout   types.Duration `json:",omitempty"`
	Discovery          DiscoveryConfig
	RedoOperation      bool `json:",omitempty"`
	Unisocket          bool `json:",omitempty"`
}

func (c *Config) Clone() Config {
	return Config{
		Name:               c.Name,
		Unisocket:          c.Unisocket,
		HeartbeatInterval:  c.HeartbeatInterval,
		HeartbeatTimeout:   c.HeartbeatTimeout,
		InvocationTimeout:  c.InvocationTimeout,
		RedoOperation:      c.RedoOperation,
		loadBalancer:       c.loadBalancer,
		Security:           c.Security.Clone(),
		Cloud:              c.Cloud.Clone(),
		Discovery:          c.Discovery.Clone(),
		ConnectionStrategy: c.ConnectionStrategy.Clone(),
		Network:            c.Network.Clone(),
	}
}

func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = defaultName
	}
	if err := validate.NonNegativeDuration(&c.HeartbeatInterval, 5*time.Second, "invalid heartbeat interval"); err != nil {
		return err
	}
	if err := validate.NonNegativeDuration(&c.HeartbeatTimeout, 60*time.Second, "invalid heartbeat timeout"); err != nil {
		return err
	}
	if err := validate.NonNegativeDuration(&c.InvocationTimeout, 120*time.Second, "invalid heartbeat timeout"); err != nil {
		return err
	}
	if c.loadBalancer == nil {
		c.loadBalancer = NewRoundRobinLoadBalancer()
	}
	if err := c.Security.Validate(); err != nil {
		return err
	}
	if err := c.Cloud.Validate(); err != nil {
		return err
	}
	if err := c.Discovery.Validate(); err != nil {
		return err
	}
	if err := c.Network.Validate(); err != nil {
		return err
	}
	if err := c.ConnectionStrategy.Validate(); err != nil {
		return err
	}
	return nil
}

// SetLoadBalancer sets the load balancer for the cluster.
// If load balancer is nil, the default load balancer is used.
func (c *Config) SetLoadBalancer(lb LoadBalancer) {
	c.loadBalancer = lb
}

// LoadBalancer returns the load balancer.
func (c *Config) LoadBalancer() LoadBalancer {
	return c.loadBalancer
}
