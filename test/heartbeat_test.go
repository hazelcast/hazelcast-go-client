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

package test

import (
	"sync"
	"testing"

	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/stretchr/testify/assert"
)

type heartbeatListener struct {
	wg *sync.WaitGroup
}

func (l *heartbeatListener) HeartbeatResumed(connection *internal.Connection) {
	l.wg.Done()

}

func (l *heartbeatListener) HeartbeatStopped(connection *internal.Connection) {
	l.wg.Done()
}

func TestHeartbeatStoppedForConnection(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	heartbeatListener := &heartbeatListener{wg: wg}
	member, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(100)
	config.SetProperty(property.HeartbeatInterval.Name(), "3000")
	config.SetProperty(property.HeartbeatTimeout.Name(), "5000")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	wg.Add(1)
	client.(*internal.HazelcastClient).HeartBeatService.AddHeartbeatListener(heartbeatListener)
	remoteController.SuspendMember(cluster.ID, member.UUID)
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, false, timeout, "heartbeatStopped listener failed")
	wg.Add(1)
	remoteController.ResumeMember(cluster.ID, member.UUID)
	timeout = WaitTimeout(wg, Timeout)
	assert.Equalf(t, false, timeout, "heartbeatRestored listener failed")
}

func TestServerShouldNotCloseClientWhenClientOnlyListening(t *testing.T) {
	heartbeatConfig, _ := Read("heartbeat_config.xml")
	cluster, _ = remoteController.CreateCluster("", heartbeatConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)

	lifecycleListener := lifecycleListener2{collector: make([]string, 0)}
	config := hazelcast.NewConfig()
	config.AddLifecycleListener(&lifecycleListener)
	config.SetProperty(property.HeartbeatInterval.Name(), "1000")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	client2, _ := hazelcast.NewClient()
	defer client2.Shutdown()
	topicName := "topicName"
	topic, _ := client.GetTopic(topicName)
	listener := &topicMessageListener{}
	topic.AddMessageListener(listener)
	topic2, _ := client2.GetTopic(topicName)
	begin := time.Now()
	for time.Since(begin) < 16*time.Second {
		topic2.Publish("message")
	}
	assert.Equal(t, len(lifecycleListener.collector), 0)

}

type topicMessageListener struct {
}

func (l *topicMessageListener) OnMessage(message core.Message) error {
	return nil
}

type lifecycleListener2 struct {
	collector []string
}

func (l *lifecycleListener2) LifecycleStateChanged(newState string) {
	if newState == core.LifecycleStateDisconnected {
		l.collector = append(l.collector, newState)

	}
}
