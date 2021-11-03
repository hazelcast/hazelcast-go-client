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

package hazelcast_test

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/log"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestDefaultConfig(t *testing.T) {
	config := hazelcast.Config{}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	checkDefault(t, &config)
}

func TestNewConfig_SetAddress(t *testing.T) {
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("192.168.1.2")
	assert.Equal(t, []string{"192.168.1.2"}, config.Cluster.Network.Addresses)
}

//newConfigValidateScenario to validate NewConfig scenarios
type newConfigValidateScenario struct {
	inputAddr         string
	inputPortRange    *cluster.PortRange
	outputAddr        string
	errEmpty          bool
	expectedPortRange cluster.PortRange
}

var validateAddressScenarios = []newConfigValidateScenario{
	{
		inputAddr:         "192.168.1.2",
		outputAddr:        "192.168.1.2:0",
		errEmpty:          true,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2:",
		outputAddr:        "192.168.1.2:",
		errEmpty:          false,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2:0",
		outputAddr:        "192.168.1.2:0",
		errEmpty:          true,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2:1234",
		outputAddr:        "192.168.1.2:1234",
		errEmpty:          true,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2:-1",
		outputAddr:        "192.168.1.2:-1",
		errEmpty:          false,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2",
		outputAddr:        "192.168.1.2:0",
		errEmpty:          true,
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5703},
	},
	{
		inputAddr:         "192.168.1.2",
		outputAddr:        "192.168.1.2:0",
		errEmpty:          true,
		inputPortRange:    &cluster.PortRange{Min: 5701, Max: 5705},
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5705},
	},
	{
		inputAddr:         "192.168.1.2:0",
		outputAddr:        "192.168.1.2:0",
		errEmpty:          true,
		inputPortRange:    &cluster.PortRange{Min: 5701, Max: 5705},
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5705},
	},
	{
		inputAddr:         "192.168.1.2",
		outputAddr:        "192.168.1.2",
		errEmpty:          false,
		inputPortRange:    &cluster.PortRange{Min: -1, Max: 5705},
		expectedPortRange: cluster.PortRange{Min: -1, Max: 5705},
	},
	{
		inputAddr:         "192.168.1.2",
		outputAddr:        "192.168.1.2",
		errEmpty:          false,
		inputPortRange:    &cluster.PortRange{Min: 5701, Max: 5700},
		expectedPortRange: cluster.PortRange{Min: 5701, Max: 5700},
	},
}

func TestNewConfig_Validate(t *testing.T) {
	for _, scenario := range validateAddressScenarios {
		config := hazelcast.NewConfig()
		config.Cluster.Network.SetAddresses(scenario.inputAddr)
		assert.Equal(t, []string{scenario.inputAddr}, config.Cluster.Network.Addresses)
		if scenario.inputPortRange != nil {
			config.Cluster.Network.SetPortRange(scenario.inputPortRange.Min, scenario.inputPortRange.Max)
		}
		err := config.Cluster.Network.Validate()
		if scenario.errEmpty {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
		assert.Equal(t, []string{scenario.outputAddr}, config.Cluster.Network.Addresses)
		assert.Equal(t, scenario.expectedPortRange, config.Cluster.Network.PortRange)
	}
}

func TestUnMarshalDefaultJSONConfig(t *testing.T) {
	var config hazelcast.Config
	if err := json.Unmarshal([]byte("{}"), &config); err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	checkDefault(t, &config)
}

func TestUnmarshalJSONConfig(t *testing.T) {
	var config hazelcast.Config
	text := `
{
	"Cluster": {
		"Name": "foo",
		"HeartbeatInterval": "10s",
		"HeartbeatTimeout": "15s",
		"InvocationTimeout": "25s",
		"Network": {
			"ConnectionTimeout": "20s"
		},
		"ConnectionStrategy": {
			"ReconnectMode": "off"
		}
	},
	"Logger2": {
		"Level": "error"
	},
	"Stats": {
		"Enabled": true,
		"Period": "2m"
	},
	"FlakeIDGenerators": {
		"bar": {
			"PrefetchCount": 42,
			"PrefetchExpiry": "42s"
		}
	}
}
`
	if err := json.Unmarshal([]byte(text), &config); err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "foo", config.Cluster.Name)
	assert.Equal(t, log.Level("error"), config.Logger.Level)
	assert.Equal(t, types.Duration(20*time.Second), config.Cluster.Network.ConnectionTimeout)
	assert.Equal(t, types.Duration(10*time.Second), config.Cluster.HeartbeatInterval)
	assert.Equal(t, types.Duration(15*time.Second), config.Cluster.HeartbeatTimeout)
	assert.Equal(t, types.Duration(25*time.Second), config.Cluster.InvocationTimeout)
	assert.Equal(t, cluster.ReconnectModeOff, config.Cluster.ConnectionStrategy.ReconnectMode)
	assert.Equal(t, true, config.Stats.Enabled)
	assert.Equal(t, types.Duration(2*time.Minute), config.Stats.Period)
	assert.Equal(t, int32(42), config.FlakeIDGenerators["bar"].PrefetchCount)
	assert.Equal(t, types.Duration(42*time.Second), config.FlakeIDGenerators["bar"].PrefetchExpiry)
}

func TestMarshalDefaultConfig(t *testing.T) {
	config := hazelcast.Config{}
	b, err := json.Marshal(&config)
	if err != nil {
		t.Fatal(err)
	}
	target := `{"Logger2":{},"Failover":{},"Serialization":{},"Cluster":{"Security":{"Credentials":{}},"Cloud":{},"Network":{"SSL":{},"PortRange":{}},"ConnectionStrategy":{"Retry":{}},"Discovery":{}},"Stats":{}}`
	assertStringEquivalent(t, target, string(b))
}

func TestValidateFlakeIDGeneratorConfig(t *testing.T) {
	testCases := []struct {
		expectErr                error
		name                     string
		config                   hazelcast.FlakeIDGeneratorConfig
		expectPrefetchExpiration types.Duration
		expectPrefetchCount      int32
	}{
		{
			name:      "NegativePrefetchCount",
			config:    hazelcast.FlakeIDGeneratorConfig{PrefetchCount: -1},
			expectErr: hzerrors.ErrIllegalArgument,
		},
		{
			name:                     "ValidPrefetchCount_1",
			config:                   hazelcast.FlakeIDGeneratorConfig{PrefetchCount: 1},
			expectPrefetchCount:      1,
			expectPrefetchExpiration: hazelcast.DefaultFlakeIDPrefetchExpiry,
		},
		{
			name:                     "ValidPrefetchCount_100_000",
			config:                   hazelcast.FlakeIDGeneratorConfig{PrefetchCount: 100_000},
			expectPrefetchCount:      100_000,
			expectPrefetchExpiration: hazelcast.DefaultFlakeIDPrefetchExpiry,
		},
		{
			name:      "InvalidPrefetchCount",
			config:    hazelcast.FlakeIDGeneratorConfig{PrefetchCount: 100_001},
			expectErr: hzerrors.ErrIllegalArgument,
		},
		{
			name:      "NegativePrefetchExpiration",
			config:    hazelcast.FlakeIDGeneratorConfig{PrefetchExpiry: types.Duration(-1)},
			expectErr: hzerrors.ErrIllegalArgument,
		},
		{
			name:                     "ZeroPrefetchExpiration",
			config:                   hazelcast.FlakeIDGeneratorConfig{PrefetchExpiry: types.Duration(0)},
			expectPrefetchCount:      hazelcast.DefaultFlakeIDPrefetchCount,
			expectPrefetchExpiration: hazelcast.DefaultFlakeIDPrefetchExpiry,
		},
		{
			name:                     "PositivePrefetchExpiration",
			config:                   hazelcast.FlakeIDGeneratorConfig{PrefetchExpiry: types.Duration(1)},
			expectPrefetchCount:      hazelcast.DefaultFlakeIDPrefetchCount,
			expectPrefetchExpiration: types.Duration(1),
		},
		{
			name:                     "ZeroValuedConfiguration",
			config:                   hazelcast.FlakeIDGeneratorConfig{},
			expectPrefetchCount:      hazelcast.DefaultFlakeIDPrefetchCount,
			expectPrefetchExpiration: hazelcast.DefaultFlakeIDPrefetchExpiry,
		},
		{
			name: "ValidCustomConfiguration",
			config: hazelcast.FlakeIDGeneratorConfig{
				PrefetchCount:  42,
				PrefetchExpiry: types.Duration(42),
			},
			expectPrefetchCount:      42,
			expectPrefetchExpiration: types.Duration(42),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Validate(); tc.expectErr != nil {
				assert.True(t, errors.Is(err, tc.expectErr))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectPrefetchCount, tc.config.PrefetchCount)
				assert.Equal(t, tc.expectPrefetchExpiration, tc.config.PrefetchExpiry)
			}
		})
	}
}

func TestConfig_AddFlakeIDGenerator(t *testing.T) {
	testCases := []struct {
		expectErr      error
		prefetchExpiry types.Duration
		expectExpiry   types.Duration
		prefetchCount  int32
		expectCount    int32
	}{
		{
			prefetchExpiry: 42,
			expectExpiry:   42,
			prefetchCount:  0,
			expectCount:    hazelcast.DefaultFlakeIDPrefetchCount,
		},
		{
			prefetchExpiry: 0,
			expectExpiry:   hazelcast.DefaultFlakeIDPrefetchExpiry,
			prefetchCount:  42,
			expectCount:    42,
		},
		{
			expectErr:      hzerrors.ErrIllegalArgument,
			prefetchExpiry: 42,
			prefetchCount:  -1,
		},
		{
			expectErr:      hzerrors.ErrIllegalArgument,
			prefetchExpiry: -1,
			prefetchCount:  42,
		},
		{
			prefetchExpiry: 42,
			expectExpiry:   42,
			prefetchCount:  42,
			expectCount:    42,
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			config := hazelcast.Config{}
			err := config.AddFlakeIDGenerator("foo", tc.prefetchCount, tc.prefetchExpiry)
			if tc.expectErr != nil {
				_, ok := config.FlakeIDGenerators["foo"]
				assert.False(t, ok)
				assert.True(t, errors.Is(err, tc.expectErr))
			} else {
				idConf, ok := config.FlakeIDGenerators["foo"]
				assert.True(t, ok)
				assert.Equal(t, tc.expectCount, idConf.PrefetchCount)
				assert.Equal(t, tc.expectExpiry, idConf.PrefetchExpiry)
			}
		})
	}
}

func TestConfig_AddExistingFlakeIDGenerator(t *testing.T) {
	config := hazelcast.Config{}
	assert.NoError(t, config.AddFlakeIDGenerator("foo", 1, 1))
	err := config.AddFlakeIDGenerator("foo", 2, 2)
	assert.True(t, errors.Is(err, hzerrors.ErrIllegalArgument))
}

func checkDefault(t *testing.T, c *hazelcast.Config) {
	assert.Equal(t, "", c.ClientName)
	assert.Equal(t, []string(nil), c.Labels)

	assert.Equal(t, "dev", c.Cluster.Name)
	assert.Equal(t, types.Duration(5*time.Second), c.Cluster.HeartbeatInterval)
	assert.Equal(t, types.Duration(60*time.Second), c.Cluster.HeartbeatTimeout)
	assert.Equal(t, types.Duration(120*time.Second), c.Cluster.InvocationTimeout)
	assert.Equal(t, false, c.Cluster.Unisocket)
	assert.Equal(t, false, c.Cluster.RedoOperation)

	assert.Equal(t, []string{"127.0.0.1:5701"}, c.Cluster.Network.Addresses)
	assert.Equal(t, false, c.Cluster.Network.SSL.Enabled)
	assert.NotNil(t, c.Cluster.Network.SSL.TLSConfig())
	assert.Equal(t, types.Duration(5*time.Second), c.Cluster.Network.ConnectionTimeout)

	assert.Equal(t, "", c.Cluster.Security.Credentials.Username)
	assert.Equal(t, "", c.Cluster.Security.Credentials.Password)

	assert.Equal(t, false, c.Cluster.Discovery.UsePublicIP)

	assert.Equal(t, false, c.Cluster.Cloud.Enabled)
	assert.Equal(t, "", c.Cluster.Cloud.Token)

	assert.Equal(t, cluster.ReconnectModeOn, c.Cluster.ConnectionStrategy.ReconnectMode)
	assert.Equal(t, types.Duration(internal.DefaultConnectionTimeoutWithoutFailover), c.Cluster.ConnectionStrategy.Timeout)
	cr := &c.Cluster.ConnectionStrategy.Retry
	assert.Equal(t, types.Duration(1*time.Second), cr.InitialBackoff)
	assert.Equal(t, types.Duration(30*time.Second), cr.MaxBackoff)
	assert.Equal(t, 1.05, cr.Multiplier)
	assert.Equal(t, 0.0, cr.Jitter)

	assert.Equal(t, int32(0), c.Serialization.PortableVersion)
	assert.Equal(t, false, c.Serialization.LittleEndian)

	assert.Equal(t, false, c.Stats.Enabled)
	assert.Equal(t, types.Duration(5*time.Second), c.Stats.Period)

	assert.Equal(t, log.InfoLevel, c.Logger.Level)

	assert.Equal(t, false, c.Failover.Enabled)
}

func assertStringEquivalent(t *testing.T, s1, s2 string) {
	assert.Equal(t, len(s1), len(s2))
	s1sl := []byte(s1)
	s2sl := []byte(s2)
	sort.Slice(s1sl, func(i, j int) bool {
		return s1sl[i] < s1sl[j]
	})
	sort.Slice(s2sl, func(i, j int) bool {
		return s2sl[i] < s2sl[j]
	})
	assert.Equal(t, s1sl, s2sl)
}
