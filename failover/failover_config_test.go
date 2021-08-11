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

package failover_test

import (
	"errors"
	"math"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/failover"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/stretchr/testify/assert"
)

func TestFailoverConfigValidate_Empty(t *testing.T) {
	c := failover.Config{}
	assert.NoError(t, c.Validate(cluster.Config{}))
}

func TestFailoverConfigValidate_ZeroTryCount(t *testing.T) {
	c := failover.Config{
		Enabled: true,
		Configs: []cluster.Config{emptyClusterConfig(), emptyClusterConfig()},
	}
	c.Validate(emptyClusterConfig())
	assert.Equal(t, math.MaxInt32, c.TryCount)
}

func TestFailoverConfigValidate_NoConfigs(t *testing.T) {
	c := failover.Config{
		Enabled:  true,
		TryCount: 1,
	}
	if !errors.Is(c.Validate(emptyClusterConfig()), hzerrors.ErrIllegalArgument) {
		t.Fatalf("should fail as ErrIllegalArgument")
	}
}

func TestFailoverConfigValidate_SameConfigs(t *testing.T) {
	c := failover.Config{
		Enabled:  true,
		TryCount: 1,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	assert.NoError(t, c.Validate(emptyClusterConfig()))
}

func TestFailoverConfigValidate_ConfigsWithAllowedDifferences(t *testing.T) {
	c := failover.Config{
		Enabled:  true,
		TryCount: 42,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	assert.NoError(t, c.Validate(allowedClusterConfig()))
}

func TestFailoverConfigValidate_ConfigsWithUnallowedDifferences(t *testing.T) {
	rootConfig := allowedClusterConfig()
	rootConfig.InvocationTimeout = 42
	c := failover.Config{
		Enabled:  true,
		TryCount: 42,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	if !errors.Is(c.Validate(rootConfig), hzerrors.ErrIllegalArgument) {
		t.Fatalf("should fail as ErrIllegalArgument")
	}
}

func emptyClusterConfig() cluster.Config {
	cc := cluster.Config{}
	cc.Validate() // the struct is mutated here
	return cc
}

// generates cluster config with non-default values in allowed options
func allowedClusterConfig() cluster.Config {
	cc := cluster.Config{
		Name: "foo",
		Security: cluster.SecurityConfig{
			Credentials: cluster.CredentialsConfig{
				Username: "user1",
				Password: "pwd1",
			},
		},
		Network: cluster.NetworkConfig{
			SSL:       cluster.SSLConfig{Enabled: true},
			Addresses: []string{"member1"},
		},
		Cloud: cluster.CloudConfig{Enabled: true, Token: "token1"},
	}
	cc.Validate() // the struct is mutated here
	return cc
}
