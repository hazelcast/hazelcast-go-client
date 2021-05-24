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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

func TestDefaultConfig(t *testing.T) {
	config := hazelcast.NewConfig()
	assert.Equal(t, config.LoggerConfig.Level, logger.InfoLevel)
	assert.Equal(t, config.ClusterConfig.Name, "dev")
	assert.Equal(t, config.ClusterConfig.Addrs, []string{"127.0.0.1:5701"})
}

func TestNewConfig(t *testing.T) {
	config := hazelcast.NewConfig()
	if err := config.ClusterConfig.SetAddress("192.168.1.2"); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []string{"192.168.1.2"}, config.ClusterConfig.Addrs)
}
