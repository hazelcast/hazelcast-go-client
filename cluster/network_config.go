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
	validate "github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var ApplyDefaultsPort = 0
var defaultPort = 5701
var defaultAddress = fmt.Sprintf("127.0.0.1:%d", defaultPort)

type NetworkConfig struct {
	PortRange         *PortRange     `json:",omitempty"`
	SSL               SSLConfig      `json:",omitempty"`
	Addresses         []string       `json:",omitempty"`
	ConnectionTimeout types.Duration `json:",omitempty"`
}

type PortRange struct {
	Min int32 `json:",omitempty"`
	Max int32 `json:",omitempty"`
}

func (pr *PortRange) Clone() *PortRange {
	return &PortRange{
		Min: pr.Min,
		Max: pr.Max,
	}
}

func (c *NetworkConfig) Clone() NetworkConfig {
	addrs := make([]string, len(c.Addresses))
	copy(addrs, c.Addresses)
	var portRange *PortRange = nil
	if c.PortRange != nil {
		portRange = c.PortRange.Clone()
	}
	return NetworkConfig{
		Addresses:         addrs,
		ConnectionTimeout: c.ConnectionTimeout,
		SSL:               c.SSL.Clone(),
		PortRange:         portRange,
	}
}

func (c *NetworkConfig) SetPortRange(min int, max int) {
	c.PortRange = &PortRange{
		Min: int32(min),
		Max: int32(max),
	}
}

func (c *NetworkConfig) Validate() error {
	// validate port range
	if err := c.validatePortRange(); err != nil {
		return err
	}

	// validate addresses
	if len(c.Addresses) == 0 {
		c.Addresses = []string{defaultAddress}
	} else {
		for i, addr := range c.Addresses {
			host, port, err := internal.ParseAddr(addr)
			if err != nil {
				return fmt.Errorf("invalid address '%s': %w", addr, err)
			}
			if port == 0 && c.PortRange == nil {
				c.Addresses[i] = fmt.Sprintf("%s:%d", host, defaultPort)
			} else if port == 0 && c.PortRange != nil {
				c.Addresses[i] = fmt.Sprintf("%s:%d", host, ApplyDefaultsPort)
			}
		}
	}
	if err := validate.NonNegativeDuration(&c.ConnectionTimeout, 5*time.Second, "invalid connection timeout"); err != nil {
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
	if c.PortRange != nil {
		if c.PortRange.Min > 0 && c.PortRange.Max > c.PortRange.Min {
			return nil
		}
		return fmt.Errorf("invalid port range: '%d-%d'", c.PortRange.Min, c.PortRange.Max)
	}
	return nil
}
