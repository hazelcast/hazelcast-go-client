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
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

func TestConfigBuilderDefault(t *testing.T) {
	configBuilder := hazelcast.NewConfigBuilder()
	if config, err := configBuilder.Config(); err != nil {
		t.Fatal(err)
	} else {
		it.AssertEquals(t, config.LoggerConfig.Level, logger.InfoLevel)
		it.AssertEquals(t, config.ClusterConfig.Name, "dev")
		it.AssertEquals(t, config.ClusterConfig.Addrs, []string{"localhost:5701"})
	}
}

func TestConfigNewConfigBuilder(t *testing.T) {
	configBuilder := hazelcast.NewConfigBuilder()
	configBuilder.Cluster().
		SetAddrs("192.168.1.2").
		SetName("my-cluster")
	config, err := configBuilder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	if config.ClusterConfig.Name != "my-cluster" {
		t.Errorf("target: my-cluster != %v", config.ClusterConfig.Name)
	}
	targetAddrs := []string{"192.168.1.2"}
	if !reflect.DeepEqual(targetAddrs, config.ClusterConfig.Addrs) {
		t.Errorf("target: %v != %v", targetAddrs, config.ClusterConfig.Addrs)
	}
}
