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

package proxy

import (
	"github.com/hazelcast/hazelcast-go-client"
	"testing"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	"log"
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/tests"
)

var mp IMap
var mp2 IMap
var rmp ReplicatedMap
var client hazelcast.IHazelcastInstance

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()

	initialize()
	predicateTestInit()
	m.Run()
	clear()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func initialize() {
	mp, _ = client.GetMap("myMap")
	rmp, _ = client.GetReplicatedMap("myReplicatedMap")
}

func clear() {
	mp.Clear()
	rmp.Clear()
}
