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

	"github.com/hazelcast/hazelcast-go-client"
)

func TestClient_NewConfigBuilder(t *testing.T) {
	builder := hazelcast.NewConfigBuilder()
	builder.Cluster().
		SetMembers("192.168.1.1").
		SetName("my-cluster")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
	}
	if "my-cluster" != config.ClusterConfig.Name {
		t.Errorf("target: %v != %v", "my-cluster", config.ClusterConfig.Name)
	}
}

/*
func TestClient_NewClientWithConfig(t *testing.T) {
	builder := hazelcast.NewConfigBuilder()
	builder.SetClientName("my-client")
	hz := it.MustClient(hazelcast.StartNewClientWithConfig(builder))
	defer hz.Shutdown()
	targetClientName := "my-client"
	if targetClientName != hz.Name() {
		t.Errorf("target: %v != %v", targetClientName, hz.Name())
	}
}


*/
