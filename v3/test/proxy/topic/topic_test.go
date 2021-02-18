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

package topic

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/core"
	"github.com/hazelcast/hazelcast-go-client/v3/rc"
	"github.com/hazelcast/hazelcast-go-client/v3/test/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

var topic core.Topic
var client hazelcast.Client

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClient()
	topic, _ = client.GetTopic("myTopic")
	m.Run()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestTopicProxy_AddListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &topicMessageListener{wg: wg}
	registrationID, err := topic.AddMessageListener(listener)
	defer topic.RemoveMessageListener(registrationID)
	require.NoError(t, err)
	topic.Publish("item-value")
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "topic AddMembershipListener() failed")
	assert.Equalf(t, listener.msg, "item-value", "topic AddMembershipListener() failed")
	if !listener.publishTime.After(time.Time{}) {
		t.Fatal("publishTime should be greater than 0")
	}
}

func TestTopicProxy_RemoveListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	wg.Add(1)
	listener := &topicMessageListener{wg: wg}
	registrationID, err := topic.AddMessageListener(listener)
	require.NoError(t, err)
	removed, err := topic.RemoveMessageListener(registrationID)
	require.NoError(t, err)
	assert.Equalf(t, removed, true, "topic RemoveMembershipListener() failed")
	topic.Publish("item-value")
	timeout := testutil.WaitTimeout(wg, testutil.TimeoutShort)
	assert.Equalf(t, true, timeout, "topic RemoveMembershipListener() failed")
}

func TestTopicProxy_PublishNilMessage(t *testing.T) {
	err := topic.Publish(nil)
	require.Errorf(t, err, "nil message should return an error")
}

type topicMessageListener struct {
	wg          *sync.WaitGroup
	msg         interface{}
	publishTime time.Time
}

func (l *topicMessageListener) OnMessage(message core.Message) error {
	l.msg = message.MessageObject()
	l.publishTime = message.PublishTime()
	l.wg.Done()
	return nil
}
