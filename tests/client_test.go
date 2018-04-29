// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

func TestClientGetMapWhenNoMemberUp(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	remoteController.ShutdownCluster(cluster.ID)
	_, err := client.Map("map")
	assert.ErrorNotNil(t, err, "getMap should have returned an error when no member is up")
	client.Shutdown()
}
