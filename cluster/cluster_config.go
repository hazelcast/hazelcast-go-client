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
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	defaultName    = "dev"
	defaultAddress = "127.0.0.1:5701"
)

type Config struct {
	loadBalancer      LoadBalancer
	Security          SecurityConfig       `json:",omitempty"`
	SSL               SSLConfig            `json:",omitempty"`
	Name              string               `json:",omitempty"`
	HazelcastCloud    HazelcastCloudConfig `json:",omitempty"`
	Address           []string             `json:",omitempty"`
	InvocationTimeout types.Duration       `json:",omitempty"`
	HeartbeatInterval types.Duration       `json:",omitempty"`
	HeartbeatTimeout  types.Duration       `json:",omitempty"`
	ConnectionTimeout types.Duration       `json:",omitempty"`
	Discovery         DiscoveryConfig      `json:",omitempty"`
	RedoOperation     bool                 `json:",omitempty"`
	Unisocket         bool                 `json:",omitempty"`
}

func (c *Config) Clone() Config {
	addrs := make([]string, len(c.Address))
	copy(addrs, c.Address)
	return Config{
		Name:              c.Name,
		Address:           addrs,
		Unisocket:         c.Unisocket,
		ConnectionTimeout: c.ConnectionTimeout,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTimeout:  c.HeartbeatTimeout,
		InvocationTimeout: c.InvocationTimeout,
		RedoOperation:     c.RedoOperation,
		loadBalancer:      c.loadBalancer,
		Security:          c.Security.Clone(),
		SSL:               c.SSL.Clone(),
		HazelcastCloud:    c.HazelcastCloud.Clone(),
		Discovery:         c.Discovery.Clone(),
	}
}

func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = defaultName
	}
	if len(c.Address) == 0 {
		c.Address = []string{defaultAddress}
	} else {
		for _, addr := range c.Address {
			if err := checkAddress(addr); err != nil {
				return fmt.Errorf("invalid address %s: %w", addr, err)
			}
		}
	}
	if c.ConnectionTimeout <= 0 {
		c.ConnectionTimeout = types.Duration(5 * time.Second)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = types.Duration(5 * time.Second)
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = types.Duration(60 * time.Second)
	}
	if c.InvocationTimeout <= 0 {
		c.InvocationTimeout = types.Duration(120 * time.Second)
	}
	if err := c.Security.Validate(); err != nil {
		return err
	}
	if err := c.SSL.Validate(); err != nil {
		return err
	}
	if err := c.HazelcastCloud.Validate(); err != nil {
		return err
	}
	if err := c.Discovery.Validate(); err != nil {
		return err
	}
	return nil
}

// SetAddress sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
func (c *Config) SetAddress(addrs ...string) error {
	for _, addr := range addrs {
		if err := checkAddress(addr); err != nil {
			return fmt.Errorf("invalid address %s: %w", addr, err)
		}
	}
	c.Address = addrs
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

func checkAddress(addr string) error {
	_, _, err := internal.ParseAddr(addr)
	return err
}
