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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
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
	if err := config.Cluster.SetAddress("192.168.1.2"); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []string{"192.168.1.2"}, config.Cluster.Address)
}

func TestJSONConfig_Default(t *testing.T) {
	var config hazelcast.Config
	if err := json.Unmarshal([]byte("{}"), &config); err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	checkDefault(t, &config)
}

func TestJSONConfig(t *testing.T) {
	var config hazelcast.Config
	text := `
{
	"Cluster": {
		"Name": "foo",
		"HeartbeatInterval": "10s"
	},
	"Logger": {
		"Level": "error"
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
	assert.Equal(t, 10*time.Second, config.Cluster.HeartbeatInterval)
	assert.Equal(t, logger.Level("error"), config.Logger.Level)
}

func checkDefault(t *testing.T, c *hazelcast.Config) {
	assert.Equal(t, logger.InfoLevel, c.Logger.Level)
	assert.Equal(t, "dev", c.Cluster.Name)
	assert.Equal(t, []string{"127.0.0.1:5701"}, c.Cluster.Address)
	assert.Equal(t, 5*time.Second, c.Cluster.ConnectionTimeout)
	assert.Equal(t, 5*time.Second, c.Cluster.HeartbeatInterval)
	assert.Equal(t, 60*time.Second, c.Cluster.HeartbeatTimeout)
	assert.Equal(t, 120*time.Second, c.Cluster.InvocationTimeout)
	assert.Equal(t, false, c.Cluster.Unisocket)
	assert.NotNil(t, c.Cluster.SSLConfig.TLSConfig())

}
