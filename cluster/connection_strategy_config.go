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

// ReconnectMode enables or disables reconnecting to a cluster.
type ReconnectMode int

func (rm *ReconnectMode) UnmarshalText(b []byte) error {
	text := string(b)
	switch text {
	case "on":
		*rm = ReconnectModeOn
	case "off":
		*rm = ReconnectModeOff
	default:
		return fmt.Errorf("invalid reconnect mode %s: %w", text, hzerrors.ErrIllegalState)
	}
	return nil
}

func (rm ReconnectMode) MarshalText() ([]byte, error) {
	switch rm {
	case ReconnectModeOn:
		return []byte("on"), nil
	case ReconnectModeOff:
		return []byte("off"), nil
	}
	return nil, hzerrors.ErrIllegalState
}

const (
	// ReconnectModeOn enables reconnecting to a cluster.
	ReconnectModeOn ReconnectMode = iota
	// ReconnectModeOff disables reconnecting to a cluster.
	ReconnectModeOff
)

// ConnectionStrategyConfig contains configuration for reconnecting to a cluster.
type ConnectionStrategyConfig struct {
	// Retry contains the backoff configuration.
	Retry ConnectionRetryConfig
	// Timeout is the maximum time before giving up reconnecting to a cluster.
	Timeout types.Duration `json:",omitempty"`
	// ReconnectMode enables or disables reconnecting to a cluster.
	ReconnectMode ReconnectMode `json:",omitempty"`
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

/*
ConnectionRetryConfig contains configuration to computer the waiting the duration between connection attempts.

The waiting duration before the next reconnection attempt is found using the following formula:

	backoff = minimum(MaxBackoff, InitialBackoff)
	duration = backoff + backoff*Jitter*2.0*(RandomFloat64()-1.0)
	next(backoff) = minimum(MaxBackoff, backoff*Multiplier)

*/
type ConnectionRetryConfig struct {
	// InitialBackoff is the duration to wait for before the first reconnection attempt.
	// Defaults to 1 second.
	InitialBackoff types.Duration `json:",omitempty"`
	// MaxBackoff is the maximum duration to wait for before the next reconnection attempt.
	// Defaults to 30 seconds.
	MaxBackoff types.Duration `json:",omitempty"`
	// Multiplier controls the speed of increasing backoff duration.
	// Defaults to 1.05.
	// Should be greater than or equal to 1.
	Multiplier float64 `json:",omitempty"`
	// Jitter controls the amount of randomness introduces to reduce contention.
	// Defaults to 0.
	Jitter float64 `json:",omitempty"`
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
