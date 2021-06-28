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
)

const (
	defaultName    = "dev"
	defaultAddress = "127.0.0.1:5701"
)

type Config struct {
	SecurityConfig       SecurityConfig
	SSLConfig            SSLConfig
	Name                 string
	HazelcastCloudConfig HazelcastCloudConfig
	Address              []string
	InvocationTimeout    time.Duration
	HeartbeatInterval    time.Duration
	HeartbeatTimeout     time.Duration
	ConnectionTimeout    time.Duration
	DiscoveryConfig      DiscoveryConfig
	RedoOperation        bool
	Unisocket            bool
}

func (c *Config) Clone() Config {
	addrs := make([]string, len(c.Address))
	copy(addrs, c.Address)
	return Config{
		Name:                 c.Name,
		Address:              addrs,
		Unisocket:            c.Unisocket,
		ConnectionTimeout:    c.ConnectionTimeout,
		HeartbeatInterval:    c.HeartbeatInterval,
		HeartbeatTimeout:     c.HeartbeatTimeout,
		InvocationTimeout:    c.InvocationTimeout,
		RedoOperation:        c.RedoOperation,
		SecurityConfig:       c.SecurityConfig.Clone(),
		SSLConfig:            c.SSLConfig.Clone(),
		HazelcastCloudConfig: c.HazelcastCloudConfig.Clone(),
		DiscoveryConfig:      c.DiscoveryConfig.Clone(),
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
		c.ConnectionTimeout = 5 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 60 * time.Second
	}
	if c.InvocationTimeout <= 0 {
		c.InvocationTimeout = 120 * time.Second
	}
	if err := c.SecurityConfig.Validate(); err != nil {
		return err
	}
	if err := c.SSLConfig.Validate(); err != nil {
		return err
	}
	if err := c.HazelcastCloudConfig.Validate(); err != nil {
		return err
	}
	if err := c.DiscoveryConfig.Validate(); err != nil {
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

func checkAddress(addr string) error {
	_, _, err := internal.ParseAddr(addr)
	return err
}
