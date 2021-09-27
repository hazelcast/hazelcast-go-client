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

package cluster_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/assert"
)

func TestFailoverConfigValidate_Empty(t *testing.T) {
	c := cluster.FailoverConfig{}
	assert.NoError(t, c.Validate(cluster.Config{}))
}

func TestFailoverConfigValidate_ZeroTryCount(t *testing.T) {
	c := cluster.FailoverConfig{
		Enabled: true,
		Configs: []cluster.Config{emptyClusterConfig(), emptyClusterConfig()},
	}
	c.Validate(emptyClusterConfig())
	assert.Equal(t, math.MaxInt32, c.TryCount)
}

func TestFailoverConfigValidate_NegativeTryCount(t *testing.T) {
	c := cluster.FailoverConfig{
		Enabled:  true,
		TryCount: -42,
		Configs:  []cluster.Config{emptyClusterConfig(), emptyClusterConfig()},
	}
	if !errors.Is(c.Validate(emptyClusterConfig()), hzerrors.ErrIllegalArgument) {
		t.Fatalf("should fail as ErrIllegalArgument")
	}
}

func TestFailoverConfigValidate_NoConfigs(t *testing.T) {
	c := cluster.FailoverConfig{
		Enabled:  true,
		TryCount: 1,
	}
	if !errors.Is(c.Validate(emptyClusterConfig()), hzerrors.ErrIllegalArgument) {
		t.Fatalf("should fail as ErrIllegalArgument")
	}
}

func TestFailoverConfigValidate_SameConfigs(t *testing.T) {
	c := cluster.FailoverConfig{
		Enabled:  true,
		TryCount: 1,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	assert.NoError(t, c.Validate(emptyClusterConfig()))
}

func TestFailoverConfigValidate_ConfigsWithAllowedDifferences(t *testing.T) {
	c := cluster.FailoverConfig{
		Enabled:  true,
		TryCount: 42,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	assert.NoError(t, c.Validate(allowedClusterConfig()))
}

func TestFailoverConfigValidate_ConfigsWithUnallowedDifferences(t *testing.T) {
	rootConfig := allowedClusterConfig()
	rootConfig.InvocationTimeout = 42
	c := cluster.FailoverConfig{
		Enabled:  true,
		TryCount: 42,
		Configs:  []cluster.Config{emptyClusterConfig()},
	}
	if !errors.Is(c.Validate(rootConfig), hzerrors.ErrIllegalArgument) {
		t.Fatalf("should fail as ErrIllegalArgument")
	}
}

func TestFailoverConfig_Validate_DefaultClusterTimeout(t *testing.T) {
	// if cluster timeout was not specified, it should be set to the default
	config1 := cluster.Config{}
	config2 := cluster.Config{}
	config2.ConnectionStrategy.Timeout = types.Duration(5 * time.Second)
	foConfig := cluster.FailoverConfig{}
	foConfig.SetConfigs(config1, config2)
	foConfig.Enabled = true
	rootConfig := cluster.Config{}
	if err := rootConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := foConfig.Validate(rootConfig); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, types.Duration(120*time.Second), foConfig.Configs[0].ConnectionStrategy.Timeout)
	assert.Equal(t, types.Duration(5*time.Second), foConfig.Configs[1].ConnectionStrategy.Timeout)
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
