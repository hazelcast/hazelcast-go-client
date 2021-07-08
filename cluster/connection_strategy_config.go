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

	"github.com/hazelcast/hazelcast-go-client/types"
)

type ConnectionStrategyConfig struct {
	Retry            ConnectionRetryConfig
	DisableReconnect bool `json:",omitempty"`
}

func (c ConnectionStrategyConfig) Clone() ConnectionStrategyConfig {
	return c
}

func (c *ConnectionStrategyConfig) Validate() error {
	return c.Retry.Validate()
}

type ConnectionRetryConfig struct {
	InitialBackoff types.Duration `json:",omitempty"`
	MaxBackoff     types.Duration `json:",omitempty"`
	Multiplier     float64        `json:",omitempty"`
	ConnectTimeout types.Duration `json:",omitempty"`
	Jitter         float64        `json:",omitempty"`
}

func (c ConnectionRetryConfig) Clone() ConnectionRetryConfig {
	return c
}

func (c *ConnectionRetryConfig) Validate() error {
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = types.Duration(1 * time.Second)
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = types.Duration(30 * time.Second)
	}
	if c.Multiplier <= 0 {
		c.Multiplier = 1.05
	}
	if c.ConnectTimeout <= 0 {
		// maximum duration
		c.ConnectTimeout = types.Duration(1<<63 - 1)
	}
	return nil
}
