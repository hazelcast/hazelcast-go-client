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

package topic

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/tests"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

var topic core.ITopic
var client hazelcast.IHazelcastInstance

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", tests.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewHazelcastClient()
	topic, _ = client.GetTopic("myTopic")
	m.Run()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestTopicProxy_AddListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &topicMessageListener{wg: wg}
	registrationId, err := topic.AddMessageListener(listener)
	defer topic.RemoveMessageListener(registrationId)
	assert.Nilf(t, err, nil, "topic AddListener() failed")
	topic.Publish("item-value")
	timeout := tests.WaitTimeout(wg, tests.Timeout)
	assert.Equalf(t, nil, false, timeout, "topic AddListener() failed")
	assert.Equalf(t, nil, listener.msg, "item-value", "topic AddListener() failed")
	if !listener.publishTime.After(time.Time{}) {
		t.Fatal("publishTime should be greater than 0")
	}
}

func TestTopicProxy_RemoveListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &topicMessageListener{wg: wg}
	registrationId, err := topic.AddMessageListener(listener)
	assert.Nilf(t, err, nil, "topic AddListener() failed")
	removed, err := topic.RemoveMessageListener(registrationId)
	assert.Equalf(t, err, removed, true, "topic RemoveListener() failed")
	topic.Publish("item-value")
	timeout := tests.WaitTimeout(wg, tests.Timeout/10)
	assert.Equalf(t, nil, true, timeout, "topic RemoveListener() failed")
}

type topicMessageListener struct {
	wg          *sync.WaitGroup
	msg         interface{}
	publishTime time.Time
}

func (l *topicMessageListener) OnMessage(message core.ITopicMessage) {
	l.msg = message.MessageObject()
	l.publishTime = message.PublishTime()
	l.wg.Done()

}
