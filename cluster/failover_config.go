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
	"math"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// FailoverConfig allows configuring multiple client configs to be used
// by a single client instance. The client will try to connect them in
// the given order. When the connected cluster fails or the client
// gets blacklisted from the cluster via the Management Center, the client
// will search for alternative clusters with given configs.
type FailoverConfig struct {
	// Configs is the configured list of failover cluster configurations.
	// Together with the main configuration (Cluster option), they form
	// the list of alternative cluster configs.
	//
	// The Cluster option and the cluster configurations from this list must
	// be exactly the same except the following options:
	// * Name
	// * Security
	// * Network.SSL
	// * Network.Addresses
	// * Cloud
	Configs []Config `json:",omitempty"`
	// TryCount is the count of attempts to connect to a cluster.
	//
	// For each alternative cluster, the client will try to connect to the
	// cluster respecting related ConnectionStrategy.Retry.
	//
	// When the client can not connect a cluster, it will try to connect
	// TryCount times going over the alternative client configs in a
	// round-robin fashion. This is triggered at the start and also when
	// the client disconnects from the cluster and can not connect back to
	// it by exhausting attempts described in connectionRetry config. In
	// that case, the client will continue from where it is left off in the
	// cluster configurations list, and try the next one again in round-robin
	// TryCount times.
	//
	// For example, if one failover cluster is given in the Configs list and
	// the TryCount is set as 4, the maximum number of subsequent connection
	// attempts done by the client is 4 x 2 = 8.
	//
	// When a zero value is provided, math.MaxInt32 is used instead as the
	// value for this option.
	TryCount int `json:",omitempty"`
	// Enabled is the enable failover behavior of the client.
	Enabled bool `json:",omitempty"`
}

func (c *FailoverConfig) Clone() FailoverConfig {
	newConfigs := make([]Config, len(c.Configs))
	for i, cc := range c.Configs {
		newConfigs[i] = cc.Clone()
	}
	return FailoverConfig{
		Enabled:  c.Enabled,
		TryCount: c.TryCount,
		Configs:  newConfigs,
	}
}

func (c *FailoverConfig) Validate(root Config) error {
	// TODO: move code that references root config to the main config
	if !c.Enabled {
		return nil
	}
	if c.TryCount < 0 {
		return fmt.Errorf("non-negative TryCount number expected: %w", hzerrors.ErrIllegalArgument)
	}
	if c.TryCount == 0 {
		c.TryCount = math.MaxInt32
	}
	if len(c.Configs) == 0 {
		return fmt.Errorf("at least one failover cluster config expected: %w", hzerrors.ErrIllegalArgument)
	}
	for i := range c.Configs {
		ccp := &c.Configs[i]
		// if a cluster connection timeout was not specified, set it to default.
		t := ccp.ConnectionStrategy.Timeout
		if t == 0 {
			t = types.Duration(internal.DefaultConnectionTimeoutWithFailover)
		}
		if err := ccp.Validate(); err != nil {
			return err
		}
		ccp.ConnectionStrategy.Timeout = t
		// validate diffs in the cluster configs
		if !equalConfigs(root, *ccp) {
			return fmt.Errorf("cluster config %d has unallowed differences with the first config: %w",
				i, hzerrors.ErrIllegalArgument)
		}
	}
	// root SSL configuration is not respected if failover is enabled.
	// check that root SSL configuration is not enabled if one of the failover config has SSL disabled.
	if root.Network.SSL.Enabled {
		for _, cfg := range c.Configs {
			if !cfg.Network.SSL.Enabled {
				return fmt.Errorf("root SSL configuration cannot be enabled if SSL is disabled for any of the failover clusters, use failover configurations instead: %w", hzerrors.ErrInvalidConfiguration)
			}
		}
	}
	return nil
}

// Checks for cluster config equality.
func equalConfigs(c1, c2 Config) bool {
	c1 = sanitizedConfig(c1)
	c2 = sanitizedConfig(c2)
	return reflect.DeepEqual(c1, c2)
}

// The following fields are allowed to be different:
// * Name
// * Security
// * Network.SSL
// * Network.Addresses
// * Cloud
// * ConnectionStrategy
func sanitizedConfig(c Config) Config {
	c.Name = ""
	c.Security = SecurityConfig{}
	c.Network.SSL = SSLConfig{}
	c.Network.Addresses = nil
	c.Cloud = CloudConfig{}
	c.ConnectionStrategy = ConnectionStrategyConfig{}
	c.Discovery.Strategy = nil
	return c
}

// SetConfigs sets the cluster configuration list.
func (c *FailoverConfig) SetConfigs(configs ...Config) {
	c.Configs = configs
}
