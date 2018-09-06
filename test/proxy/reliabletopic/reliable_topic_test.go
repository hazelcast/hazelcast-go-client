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

package reliabletopic

import (
	"log"
	"testing"

	"sync"

	"strconv"

	"time"

	"runtime"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/reliabletopic"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

var client hazelcast.Instance

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	config, _ := test.Read("hazelcast_topic.xml")
	cluster, _ := remoteController.CreateCluster("", config)
	remoteController.StartMember(cluster.ID)
	client, _ = hazelcast.NewClientWithConfig(initConfig())
	m.Run()
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func initConfig() *config.Config {
	cfg := hazelcast.NewConfig()
	reliableTopicCfg := config.NewReliableTopicConfig("discard")
	reliableTopicCfg.SetTopicOverloadPolicy(core.TopicOverLoadPolicyDiscardNewest)
	cfg.AddReliableTopicConfig(reliableTopicCfg)

	reliableTopicCfg2 := config.NewReliableTopicConfig("overwrite")
	reliableTopicCfg2.SetTopicOverloadPolicy(core.TopicOverLoadPolicyDiscardOldest)
	cfg.AddReliableTopicConfig(reliableTopicCfg2)

	reliableTopicCfg3 := config.NewReliableTopicConfig("error")
	reliableTopicCfg3.SetTopicOverloadPolicy(core.TopicOverLoadPolicyError)
	cfg.AddReliableTopicConfig(reliableTopicCfg3)
	return cfg
}

func TestReliableTopicProxy_AddMessageListener(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	id, err := reliableTopic.AddMessageListener(&ReliableMessageListenerMock{})
	defer reliableTopic.RemoveMessageListener(id)
	assert.ErrorNil(t, err)
}

func TestReliableTopicProxy_AddMessageListenerNil(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	_, err := reliableTopic.AddMessageListener(nil)
	assert.ErrorNotNil(t, err, "nil message listener should not be added")
}

func TestReliableTopicProxy_RemoveMessageListenerWhenExists(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	id, err := reliableTopic.AddMessageListener(&ReliableMessageListenerMock{})
	assert.ErrorNil(t, err)
	removed, err := reliableTopic.RemoveMessageListener(id)
	assert.Equal(t, err, removed, true)
}

func TestReliableTopicProxy_RemoveMessageListenerWhenDoesntExist(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	removed, err := reliableTopic.RemoveMessageListener("id")
	assert.ErrorNotNil(t, err, "")
	if removed {
		t.Error("nonexisting registration id should return error")
	}
}

func TestReliableTopicProxy_RemoveMessageListenerWhenAlreadyRemoved(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	id, err := reliableTopic.AddMessageListener(&ReliableMessageListenerMock{})
	assert.ErrorNil(t, err)
	removed, err := reliableTopic.RemoveMessageListener(id)
	assert.Equal(t, err, removed, true)
	removed, err = reliableTopic.RemoveMessageListener(id)
	assert.ErrorNotNil(t, err, "")
	if removed {
		t.Error("nonexisting registration id should return error")
	}
}

func TestReliableTopicProxy_PublishSingle(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	id, _ := reliableTopic.AddMessageListener(&ReliableMessageListenerMock{wg: wg, storedSeq: -1})
	defer reliableTopic.RemoveMessageListener(id)
	msg := "foobar"
	err := reliableTopic.Publish(msg)
	assert.ErrorNil(t, err)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, err, timeout, false)
}

func TestReliableTopicProxy_DefaultReliableMessageListener(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	id, _ := reliableTopic.AddMessageListener(&messageListenerMock{wg: wg})
	defer reliableTopic.RemoveMessageListener(id)
	msg := "foobar"
	err := reliableTopic.Publish(msg)
	assert.ErrorNil(t, err)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, err, timeout, false)
}

func TestReliableTopicProxy_PublishNil(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	err := reliableTopic.Publish(nil)
	assert.ErrorNotNil(t, err, "")
}

func TestReliableTopicProxy_PublishMany(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	wg := new(sync.WaitGroup)
	amount := 5
	wg.Add(amount)
	id, _ := reliableTopic.AddMessageListener(&ReliableMessageListenerMock{wg: wg, storedSeq: -1})
	defer reliableTopic.RemoveMessageListener(id)
	msg := "foobar"
	for i := 0; i < amount; i++ {
		err := reliableTopic.Publish(msg + strconv.Itoa(i))
		assert.ErrorNil(t, err)

	}
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, nil, timeout, false)
}

func TestReliableTopicProxy_MessageFieldSetCorrectly(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	listener := &ReliableMessageListenerMock{wg: wg, storedSeq: -1}
	id, _ := reliableTopic.AddMessageListener(listener)
	defer reliableTopic.RemoveMessageListener(id)
	msg := "foobar"
	beforePublishTime := time.Now()
	err := reliableTopic.Publish(msg)
	afterPublishTime := time.Now()
	assert.ErrorNil(t, err)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, err, timeout, false)
	assert.Equal(t, err, len(listener.messages), 1)
	assert.Equal(t, err, listener.messages[0].MessageObject(), msg)
	assert.Equal(t, err, listener.messages[0].PublishingMember(), nil)

	actualPublishTime := listener.messages[0].PublishTime()
	log.Println(actualPublishTime, " ", beforePublishTime)
	if actualPublishTime.Second() < beforePublishTime.Second() {
		t.Error("actualPublish time should be after publish")
	}

	if afterPublishTime.Second() > afterPublishTime.Second() {
		t.Error("actualPublish time should be less than afterPublishTime")
	}

}

func TestReliableTopicProxy_AlwaysStartAfterTail(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	reliableTopic.Publish("1")
	reliableTopic.Publish("2")
	reliableTopic.Publish("3")

	wg := new(sync.WaitGroup)
	wg.Add(3)
	listener := &ReliableMessageListenerMock{wg: wg, storedSeq: -1}
	id, _ := reliableTopic.AddMessageListener(listener)
	defer reliableTopic.RemoveMessageListener(id)

	reliableTopic.Publish("4")
	reliableTopic.Publish("5")
	reliableTopic.Publish("6")

	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, nil, timeout, false)

	assert.Equal(t, nil, len(listener.messages), 3)
	assert.Equal(t, nil, listener.messages[0].MessageObject(), "4")
	assert.Equal(t, nil, listener.messages[1].MessageObject(), "5")
	assert.Equal(t, nil, listener.messages[2].MessageObject(), "6")
}

func TestReliableTopicProxy_Discard(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("discard")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 10)
	topic.Ringbuffer().AddAll(items, core.OverflowPolicyFail)
	topic.Publish(11)
	seq, err := topic.Ringbuffer().TailSequence()
	assert.ErrorNil(t, err)
	item, err := topic.Ringbuffer().ReadOne(seq)
	assert.ErrorNil(t, err)
	msg := item.(*reliabletopic.Message)
	obj, _ := client.(*internal.HazelcastClient).SerializationService.ToObject(msg.Payload().(*serialization.Data))
	assert.Equal(t, nil, obj, int64(10))
}

func TestReliableTopicProxy_Overwrite(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("overwrite")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 10)
	for _, i := range items {
		topic.Publish(i)
	}
	topic.Publish(11)
	seq, err := topic.Ringbuffer().TailSequence()
	assert.ErrorNil(t, err)
	item, err := topic.Ringbuffer().ReadOne(seq)
	assert.ErrorNil(t, err)
	msg := item.(*reliabletopic.Message)
	obj, _ := client.(*internal.HazelcastClient).SerializationService.ToObject(msg.Payload().(*serialization.Data))
	assert.Equal(t, nil, obj, int64(11))
}

func TestReliableTopicProxy_Error(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("error")
	items := generateItems(client.(*internal.HazelcastClient), 10)
	for _, i := range items {
		reliableTopic.Publish(i)
	}
	err := reliableTopic.Publish(11)
	assert.ErrorNotNil(t, err, "topic overflow policy error should cause topic to error when no space")

}

func TestReliableTopicProxy_Blocking(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("blocking")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 10)
	for _, i := range items {
		topic.Publish(i)
	}
	before := time.Now()
	// this is supposed to be blocking for 3 seconds since TTL is set to 3 seconds in server connection.
	topic.Publish(11)
	timeDiff := time.Since(before)
	seq, err := topic.Ringbuffer().TailSequence()
	assert.ErrorNil(t, err)
	item, err := topic.Ringbuffer().ReadOne(seq)
	assert.ErrorNil(t, err)
	msg := item.(*reliabletopic.Message)
	obj, _ := client.(*internal.HazelcastClient).SerializationService.ToObject(msg.Payload().(*serialization.Data))
	assert.Equal(t, nil, obj, int64(11))
	if timeDiff <= 2*time.Second {
		t.Errorf("expected at least 2 seconds delay got %s", timeDiff)
	}
}

func TestReliableTopicProxy_Stale(t *testing.T) {
	reliableTopic, _ := client.GetReliableTopic("stale")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 20)
	_, err := topic.Ringbuffer().AddAll(items, core.OverflowPolicyOverwrite)
	assert.ErrorNil(t, err)
	wg := new(sync.WaitGroup)
	wg.Add(10)
	listener := &ReliableMessageListenerMock{wg: wg, isLossTolerant: true, storedSeq: 0}
	id, err := reliableTopic.AddMessageListener(listener)
	assert.ErrorNil(t, err)
	defer reliableTopic.RemoveMessageListener(id)
	timeout := test.WaitTimeout(wg, test.Timeout)
	assert.Equal(t, nil, timeout, false)
	assert.Equal(t, nil, listener.messages[9].MessageObject(), int64(20))
}

func TestReliableTopicProxy_Leakage(t *testing.T) {
	routineNumBefore := runtime.NumGoroutine()
	client2, _ := hazelcast.NewClientWithConfig(initConfig())

	reliableTopic, _ := client2.GetReliableTopic("discard")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 20)
	wg := new(sync.WaitGroup)
	wg.Add(10)
	listener := &ReliableMessageListenerMock{wg: wg, isLossTolerant: true, storedSeq: -1}
	id, _ := reliableTopic.AddMessageListener(listener)
	defer reliableTopic.RemoveMessageListener(id)
	_, err := topic.Ringbuffer().AddAll(items, core.OverflowPolicyOverwrite)
	assert.ErrorNil(t, err)
	client2.Shutdown()
	time.Sleep(4 * time.Second)
	routineNumAfter := runtime.NumGoroutine()
	if routineNumBefore != routineNumAfter {
		t.Fatalf("Expected number of routines %d, found %d", routineNumBefore, routineNumAfter)
	}
}

type ReliableMessageListenerMock struct {
	storedSeq      int64
	isLossTolerant bool
	wg             *sync.WaitGroup
	messages       []core.Message
}

func (r *ReliableMessageListenerMock) OnMessage(message core.Message) {
	r.messages = append(r.messages, message)
	r.wg.Done()
}

func (r *ReliableMessageListenerMock) RetrieveInitialSequence() int64 {
	return r.storedSeq
}

func (r *ReliableMessageListenerMock) StoreSequence(sequence int64) {
	r.storedSeq = sequence
}

func (r *ReliableMessageListenerMock) IsLossTolerant() bool {
	return r.isLossTolerant
}

type messageListenerMock struct {
	wg *sync.WaitGroup
}

func (m *messageListenerMock) OnMessage(message core.Message) {
	m.wg.Done()
}

func generateItems(client *internal.HazelcastClient, n int) []interface{} {
	items := make([]interface{}, n)
	for i := 1; i <= n; i++ {
		data, _ := client.SerializationService.ToData(i)
		items[i-1] = reliabletopic.NewMessage(data, nil)
	}
	return items
}
