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
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"sync"
	"testing"
)

type heartbeatListener struct {
	wg *sync.WaitGroup
}

func (heartbeatListener *heartbeatListener) OnHeartbeatRestored(connection *internal.Connection) {
	heartbeatListener.wg.Done()

}

func (heartbeatListener *heartbeatListener) OnHeartbeatStopped(connection *internal.Connection) {
	heartbeatListener.wg.Done()
}

func TestHeartbeatStoppedForConnection(t *testing.T) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("3.9", DefaultServerConfig)
	heartbeatListener := &heartbeatListener{wg: wg}
	member, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(100)
	config.SetHeartbeatIntervalInSeconds(3)
	config.SetHeartbeatTimeoutInSeconds(5)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	wg.Add(1)
	client.(*internal.HazelcastClient).HeartBeatService.AddHeartbeatListener(heartbeatListener)
	remoteController.SuspendMember(cluster.ID, member.UUID)
	timeout := WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "heartbeatStopped listener failed")
	remoteController.ResumeMember(cluster.ID, member.UUID)
	wg.Add(1)
	timeout = WaitTimeout(wg, Timeout)
	AssertEqualf(t, nil, false, timeout, "heartbeatRestored listener failed")
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}
