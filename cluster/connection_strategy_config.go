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

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	validate "github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type ConnectionStrategyConfig struct {
	Retry            ConnectionRetryConfig
	Timeout          types.Duration `json:",omitempty"`
	DisableReconnect bool           `json:",omitempty"`
}

func (c ConnectionStrategyConfig) Clone() ConnectionStrategyConfig {
	return c
}

func (c *ConnectionStrategyConfig) Validate() error {
	if err := validate.NonNegativeDuration(&c.Timeout, 1<<63-1, "invalid timeout"); err != nil {
		return err
	}
	return c.Retry.Validate()
}

type ConnectionRetryConfig struct {
	InitialBackoff types.Duration `json:",omitempty"`
	MaxBackoff     types.Duration `json:",omitempty"`
	Multiplier     float64        `json:",omitempty"`
	Jitter         float64        `json:",omitempty"`
}

func (c ConnectionRetryConfig) Clone() ConnectionRetryConfig {
	return c
}

func (c *ConnectionRetryConfig) Validate() error {
	if err := validate.NonNegativeDuration(&c.InitialBackoff, 1*time.Second, "invalid initial backoff"); err != nil {
		return err
	}
	if err := validate.NonNegativeDuration(&c.MaxBackoff, 30*time.Second, "invalid max backoff"); err != nil {
		return err
	}
	if c.Multiplier == 0 {
		c.Multiplier = 1.05
	}
	if c.Multiplier < 1.0 {
		return fmt.Errorf("invalid multiplier: %w", hzerrors.ErrIllegalArgument)
	}
	return nil
}
