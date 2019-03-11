// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package clusterrestart

import (
	"testing"

	"sync"

	"time"

	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var remoteController rc.RemoteController

func TestMain(m *testing.M) {
	var err error
	remoteController, err = rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func TestReliableTopicProxy_ServerRestartWhenReliableTopicListenerRegistered(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	member, _ := remoteController.StartMember(cluster.ID)
	topicName := "topic"
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	client2, _ := hazelcast.NewClientWithConfig(config)
	defer client2.Shutdown()
	topic, _ := client.GetReliableTopic(topicName)
	topic2, _ := client2.GetReliableTopic(topicName)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	listener := &messageListenerMock{wg: wg}
	id, err := topic.AddMessageListener(listener)
	require.NoError(t, err)
	defer topic.RemoveMessageListener(id)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	remoteController.StartMember(cluster.ID)
	err = topic2.Publish(1)
	assert.NoError(t, err)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equal(t, timeout, false)
}

func TestReliableTopicProxy_ServerRestartWithInvocationTimeout(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	member, _ := remoteController.StartMember(cluster.ID)
	topicName := "topic2"
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	config.SetProperty(property.InvocationTimeoutSeconds.Name(), "2")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	client2, _ := hazelcast.NewClientWithConfig(config)
	defer client2.Shutdown()
	topic, _ := client.GetReliableTopic(topicName)
	topic2, _ := client2.GetReliableTopic(topicName)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	listener := &messageListenerMock{wg: wg}
	id, err := topic.AddMessageListener(listener)
	require.NoError(t, err)
	defer topic.RemoveMessageListener(id)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	remoteController.StartMember(cluster.ID)
	time.Sleep(time.Second)
	err = topic2.Publish("message")
	assert.NoError(t, err)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equal(t, timeout, false)
}

func TestReliableTopicProxy_ServerRestartWhenDataLossWithInvocationTimeout(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	member, _ := remoteController.StartMember(cluster.ID)
	topicName := "topic3"
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	config.SetProperty(property.InvocationTimeoutSeconds.Name(), "2")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	topic, _ := client.GetReliableTopic(topicName)
	topic.Publish("message")
	topic.Publish("message")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	listener := &DurableMessageListenerMock{wg: wg, isLossTolerant: true, storedSeq: -1}
	//listener := &messageListenerMock{wg: wg}
	id, err := topic.AddMessageListener(listener)
	require.NoError(t, err)
	defer topic.RemoveMessageListener(id)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	remoteController.StartMember(cluster.ID)
	time.Sleep(time.Second)
	err = topic.Publish("message")
	assert.NoError(t, err)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equal(t, timeout, false)
}

func TestReliableTopicProxy_ServerRestartWhenDataLossWithInvocationTimeoutWhenNotLossTolerant(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	member, _ := remoteController.StartMember(cluster.ID)
	topicName := "topic3"
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(10)
	config.SetProperty(property.InvocationTimeoutSeconds.Name(), "2")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	topic, _ := client.GetReliableTopic(topicName)
	topic.Publish("message")
	topic.Publish("message")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	listener := &DurableMessageListenerMock{wg: wg, isLossTolerant: false, storedSeq: -1}
	//listener := &messageListenerMock{wg: wg}
	id, err := topic.AddMessageListener(listener)
	require.NoError(t, err)
	defer topic.RemoveMessageListener(id)
	remoteController.ShutdownMember(cluster.ID, member.UUID)
	remoteController.StartMember(cluster.ID)
	time.Sleep(time.Second)
	err = topic.Publish("message")
	assert.NoError(t, err)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout/15)
	assert.Equal(t, timeout, true)
}

type messageListenerMock struct {
	wg *sync.WaitGroup
}

func (m *messageListenerMock) OnMessage(message core.Message) error {
	m.wg.Done()
	return nil
}

type DurableMessageListenerMock struct {
	storedSeq      int64
	isLossTolerant bool
	wg             *sync.WaitGroup
	messages       []core.Message
	err            error
}

func (r *DurableMessageListenerMock) OnMessage(message core.Message) error {
	r.messages = append(r.messages, message)
	r.wg.Done()
	return r.err
}

func (r *DurableMessageListenerMock) RetrieveInitialSequence() int64 {
	return r.storedSeq
}

func (r *DurableMessageListenerMock) StoreSequence(sequence int64) {
	r.storedSeq = sequence
}

func (r *DurableMessageListenerMock) IsLossTolerant() bool {
	return r.isLossTolerant
}

func (r *DurableMessageListenerMock) IsTerminal(err error) (bool, error) {
	return false, nil
}
