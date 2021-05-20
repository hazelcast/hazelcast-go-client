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
	"errors"
	"fmt"
	"time"
)

const (
	DefaultHost = "localhost"
	DefaultPort = 5701
)

type Config struct {
	Name              string
	Addrs             []string
	SmartRouting      bool
	ConnectionTimeout time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	InvocationTimeout time.Duration
	RedoOperation     bool
	SecurityConfig    SecurityConfig
	SSLConfig         SSLConfig
}

func NewConfig() Config {
	return Config{
		Name:              "dev",
		SmartRouting:      true,
		ConnectionTimeout: 5 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout:  60 * time.Second,
		InvocationTimeout: 120 * time.Second,
		SecurityConfig:    NewSecurityConfig(),
		SSLConfig:         NewSSLConfig(),
	}
}

func (c *Config) Clone() Config {
	addrs := make([]string, len(c.Addrs))
	copy(addrs, c.Addrs)
	return Config{
		Name:              c.Name,
		Addrs:             addrs,
		SmartRouting:      c.SmartRouting,
		ConnectionTimeout: c.ConnectionTimeout,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTimeout:  c.HeartbeatTimeout,
		InvocationTimeout: c.InvocationTimeout,
		RedoOperation:     c.RedoOperation,
		SecurityConfig:    c.SecurityConfig.Clone(),
		SSLConfig:         c.SSLConfig.Clone(),
	}
}

func (c *Config) Validate() error {
	if c.Name == "" {
		return errors.New("cluster name cannot be blank")
	}
	return nil
}

// AddAddrs sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
// By default localhost:5701 is set as the member address.
func (c *Config) AddAddrs(addrs ...string) error {
	for _, addr := range addrs {
		if err := checkAddress(addr); err != nil {
			return fmt.Errorf("invalid address %s: %w", addr, err)
		}
		c.Addrs = append(c.Addrs, addr)
	}
	return nil
}

func checkAddress(addr string) error {
	return nil
}
