/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const defaultName = "dev"

// Config contains cluster and connection configuration.
type Config struct {
	loadBalancer LoadBalancer
	// Security contains security related configuration such as credentials.
	Security SecurityConfig
	// Name is the cluster name.
	Name string `json:",omitempty"`
	// Cloud contains Hazelcast Cloud related configuration.
	Cloud CloudConfig
	// Network contains connection configuration.
	Network NetworkConfig
	// ConnectionStrategy contains cluster connection strategy configuration.
	ConnectionStrategy ConnectionStrategyConfig
	// InvocationTimeout is the maximum time to wait for the response of an invocation.
	InvocationTimeout types.Duration `json:",omitempty"`
	// HeartbeatInterval is the frequency of sending pings to the cluster to keep the connection alive.
	HeartbeatInterval types.Duration `json:",omitempty"`
	// HeartbeatTimeout is the maximum time to wait for the response of a ping before closing the connection.
	HeartbeatTimeout types.Duration `json:",omitempty"`
	// Discovery contains configuration related to discovery of Hazelcast members.
	Discovery DiscoveryConfig
	// RedoOperation enables retrying some errors even when they are not retried by default.
	RedoOperation bool `json:",omitempty"`
	// Unisocket disables smart routing and enables unisocket mode of operation.
	Unisocket bool `json:",omitempty"`
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
	err := check.EnsureNonNegativeDuration((*time.Duration)(&c.HeartbeatInterval), 5*time.Second, "invalid heartbeat interval")
	if err != nil {
		return err
	}
	err = check.EnsureNonNegativeDuration((*time.Duration)(&c.HeartbeatTimeout), 60*time.Second, "invalid heartbeat timeout")
	if err != nil {
		return err
	}
	err = check.EnsureNonNegativeDuration((*time.Duration)(&c.InvocationTimeout), 120*time.Second, "invalid heartbeat timeout")
	if err != nil {
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
	if c.ConnectionStrategy.Timeout == 0 {
		// infinity
		c.ConnectionStrategy.Timeout = types.Duration(internal.DefaultConnectionTimeoutWithoutFailover)
	}
	if c.Cloud.Enabled {
		c.Network.SSL.Enabled = true
		if c.Network.SSL.tlsConfig.ServerName == "" {
			c.Network.SSL.tlsConfig.ServerName = "hazelcast.cloud"
		}
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
