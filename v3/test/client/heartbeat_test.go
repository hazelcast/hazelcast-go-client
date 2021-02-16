// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package client

import (
	"testing"

	"time"

	"sync"

	"github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/config/property"
	"github.com/hazelcast/hazelcast-go-client/v3/core"
	"github.com/hazelcast/hazelcast-go-client/v3/test/testutil"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeatStoppedForConnection(t *testing.T) {
	wg := new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	member, _ := remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(100)
	config.SetProperty(property.HeartbeatInterval.Name(), "2000")
	config.SetProperty(property.HeartbeatTimeout.Name(), "4000")
	listener := &lifecycleListener2{wg: wg, shouldWait: true}
	config.AddLifecycleListener(listener)
	client, _ := hazelcast.NewClientWithConfig(config)
	wg.Add(1)
	defer client.Shutdown()
	remoteController.SuspendMember(cluster.ID, member.UUID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	remoteController.ResumeMember(cluster.ID, member.UUID)
	assert.False(t, timeout)
	assert.Len(t, listener.collector, 1)
}

func TestServerShouldNotCloseClientWhenClientOnlyListening(t *testing.T) {
	heartbeatConfig, _ := testutil.Read("heartbeat_config.xml")
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
	collector  []string
	wg         *sync.WaitGroup
	shouldWait bool
}

func (l *lifecycleListener2) LifecycleStateChanged(newState string) {
	if newState == core.LifecycleStateDisconnected {
		l.collector = append(l.collector, newState)
		if l.shouldWait {
			l.wg.Done()
		}
	}
}
