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

const defaultAddress = "127.0.0.1:5701"

type NetworkConfig struct {
	SSL               SSLConfig      `json:",omitempty"`
	Address           []string       `json:",omitempty"`
	ConnectionTimeout types.Duration `json:",omitempty"`
}

func (c *NetworkConfig) Clone() NetworkConfig {
	addrs := make([]string, len(c.Address))
	copy(addrs, c.Address)
	return NetworkConfig{
		Address:           addrs,
		ConnectionTimeout: c.ConnectionTimeout,
		SSL:               c.SSL.Clone(),
	}
}

func (c *NetworkConfig) Validate() error {
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
	return nil
}

// SetAddress sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
func (c *NetworkConfig) SetAddress(addrs ...string) {
	c.Address = addrs
}

func checkAddress(addr string) error {
	_, _, err := internal.ParseAddr(addr)
	return err
}
