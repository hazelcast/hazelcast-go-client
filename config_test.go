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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
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
		}
	},
	"Logger": {
		"Level": "error"
	},
	"Stats": {
		"Enabled": true,
		"Period": "2m"
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
	assert.Equal(t, logger.Level("error"), config.Logger.Level)
	assert.Equal(t, types.Duration(20*time.Second), config.Cluster.Network.ConnectionTimeout)
	assert.Equal(t, types.Duration(10*time.Second), config.Cluster.HeartbeatInterval)
	assert.Equal(t, types.Duration(15*time.Second), config.Cluster.HeartbeatTimeout)
	assert.Equal(t, types.Duration(25*time.Second), config.Cluster.InvocationTimeout)
	assert.Equal(t, true, config.Stats.Enabled)
	assert.Equal(t, types.Duration(2*time.Minute), config.Stats.Period)
}

func TestMarshalDefaultConfig(t *testing.T) {
	config := hazelcast.Config{}
	b, err := json.Marshal(&config)
	if err != nil {
		t.Fatal(err)
	}
	target := `{"Logger":{},"Serialization":{},"Cluster":{"Security":{"Credentials":{}},"Cloud":{},"Discovery":{},"Network":{"SSL":{}}},"Stats":{}}`
	assertStringEquivalent(t, target, string(b))
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

	assert.Equal(t, int32(0), c.Serialization.PortableVersion)
	assert.Equal(t, false, c.Serialization.LittleEndian)

	assert.Equal(t, false, c.Stats.Enabled)
	assert.Equal(t, types.Duration(5*time.Second), c.Stats.Period)

	assert.Equal(t, logger.InfoLevel, c.Logger.Level)

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
