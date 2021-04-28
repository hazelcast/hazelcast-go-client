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
)

const (
	DefaultHost = "localhost"
	DefaultPort = 5701
)

type SecurityConfig struct {
	Username string
	Password string
}

func (c SecurityConfig) Clone() SecurityConfig {
	return SecurityConfig{
		Username: c.Username,
		Password: c.Password,
	}
}

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
}

func (c Config) Clone() Config {
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
	}
}
