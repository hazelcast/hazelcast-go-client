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
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	defaultAddress      = "127.0.0.1:5701"
	defaultPortRangeMin = 5701
	defaultPortRangeMax = 5703
)

type NetworkConfig struct {
	SSL               SSLConfig      `json:",omitempty"`
	Addresses         []string       `json:",omitempty"`
	PortRange         PortRange      `json:",omitempty"`
	ConnectionTimeout types.Duration `json:",omitempty"`
}

type PortRange struct {
	Min int `json:",omitempty"`
	Max int `json:",omitempty"`
}

func (pr *PortRange) Clone() PortRange {
	return PortRange{
		Min: pr.Min,
		Max: pr.Max,
	}
}

func (c *NetworkConfig) Clone() NetworkConfig {
	addrs := make([]string, len(c.Addresses))
	copy(addrs, c.Addresses)
	return NetworkConfig{
		Addresses:         addrs,
		ConnectionTimeout: c.ConnectionTimeout,
		SSL:               c.SSL.Clone(),
		PortRange:         c.PortRange.Clone(),
	}
}

func (c *NetworkConfig) SetPortRange(min int, max int) {
	c.PortRange = PortRange{
		Min: min,
		Max: max,
	}
}

func (c *NetworkConfig) Validate() error {
	// set port range defaults if needed
	if c.PortRange.Min == 0 && c.PortRange.Max == 0 {
		c.PortRange = PortRange{
			Min: defaultPortRangeMin,
			Max: defaultPortRangeMax,
		}
	}
	if err := c.validatePortRange(); err != nil {
		return err
	}
	if len(c.Addresses) == 0 {
		c.Addresses = []string{defaultAddress}
	} else {
		for _, addr := range c.Addresses {
			if _, _, err := internal.ParseAddr(addr); err != nil {
				return fmt.Errorf("invalid address '%s': %w", addr, err)
			}
		}
	}
	if err := check.EnsureNonNegativeDuration((*time.Duration)(&c.ConnectionTimeout), 5*time.Second, "invalid connection timeout"); err != nil {
		return err
	}
	if err := c.SSL.Validate(); err != nil {
		return err
	}
	return nil
}

// SetAddresses sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
func (c *NetworkConfig) SetAddresses(addrs ...string) {
	c.Addresses = addrs
}

// validatePortRange validates whether the port range given is valid or not
func (c *NetworkConfig) validatePortRange() error {
	if c.PortRange.Min > 0 && c.PortRange.Max > c.PortRange.Min {
		return nil
	}
	return fmt.Errorf("invalid port range: %d-%d: %w", c.PortRange.Min, c.PortRange.Max, hzerrors.ErrIllegalArgument)

}
