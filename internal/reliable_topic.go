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

package internal

import (
	"time"

	"sync/atomic"

	"sync"

	"strings"

	"strconv"

	"log"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/iputil"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/reliabletopic"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	topicRBPrefix  = "_hz_rb_"
	maxBackoff     = 2000 * time.Millisecond
	initialBackoff = 100 * time.Millisecond
)

type ReliableTopicProxy struct {
	*proxy
	ringBuffer           core.Ringbuffer
	serializationService *serialization.Service
	topicOverLoadPolicy  core.TopicOverloadPolicy
	config               *config.ReliableTopicConfig
	msgProcessorsMu      *sync.Mutex
	msgProcessors        map[string]*messageProcessor
}

func newReliableTopicProxy(client *HazelcastClient, serviceName string, name string) (*ReliableTopicProxy, error) {
	proxy := &ReliableTopicProxy{
		msgProcessors:   make(map[string]*messageProcessor),
		msgProcessorsMu: new(sync.Mutex),
		proxy: &proxy{
			client:      client,
			serviceName: serviceName,
			name:        name,
		},
	}
	proxy.serializationService = client.SerializationService
	proxy.config = client.ClientConfig.GetReliableTopicConfig(name)
	proxy.topicOverLoadPolicy = proxy.config.TopicOverloadPolicy()
	var err error
	proxy.ringBuffer, err = client.GetRingbuffer(topicRBPrefix + name)
	return proxy, err
}

func (r *ReliableTopicProxy) AddMessageListener(messageListener core.MessageListener) (registrationID string, err error) {
	if messageListener == nil {
		return "", core.NewHazelcastNilPointerError(bufutil.NilListenerIsNotAllowed, nil)
	}
	uuid, _ := iputil.NewUUID()
	reliableMsgListener := r.toReliableMessageListener(messageListener)
	msgProcessor := newMessageProcessor(uuid, reliableMsgListener, r)
	r.msgProcessorsMu.Lock()
	r.msgProcessors[uuid] = msgProcessor
	r.msgProcessorsMu.Unlock()
	go msgProcessor.next()
	return uuid, nil
}

func (r *ReliableTopicProxy) toReliableMessageListener(messageListener core.MessageListener) core.ReliableMessageListener {
	if listener, ok := messageListener.(core.ReliableMessageListener); ok {
		return listener
	}
	return newDefaultReliableTopicMessageListener(messageListener)
}

func (r *ReliableTopicProxy) RemoveMessageListener(registrationID string) (removed bool, err error) {
	r.msgProcessorsMu.Lock()
	defer r.msgProcessorsMu.Unlock()
	if msgProcessor, ok := r.msgProcessors[registrationID]; ok {
		msgProcessor.cancel()
		return true, nil
	}
	return false, core.NewHazelcastIllegalArgumentError("no listener is found with the given id : "+
		registrationID, nil)
}

func (r *ReliableTopicProxy) Publish(message interface{}) (err error) {
	messageData, err := r.validateAndSerialize(message)
	if err != nil {
		return err
	}
	reliableMsg := reliabletopic.NewMessage(messageData, nil)
	switch r.topicOverLoadPolicy {
	case core.TopicOverLoadPolicyError:
		err = r.addOrFail(reliableMsg)
	case core.TopicOverLoadPolicyBlock:
		err = r.addWithBackoff(reliableMsg)
	case core.TopicOverLoadPolicyDiscardNewest:
		_, err = r.ringBuffer.Add(reliableMsg, core.OverflowPolicyFail)
	case core.TopicOverLoadPolicyDiscardOldest:
		err = r.addOrOverwrite(reliableMsg)
	}
	return err
}

func (r *ReliableTopicProxy) addOrFail(message *reliabletopic.Message) (err error) {
	seqID, err := r.ringBuffer.Add(message, core.OverflowPolicyFail)
	if err != nil {
		return err
	}
	if seqID == -1 {
		// TODO :: add message to error string
		errorMsg := "failed to publish message to topic: " + r.name
		return core.NewHazelcastTopicOverflowError(errorMsg, nil)
	}
	return
}

func (r *ReliableTopicProxy) addWithBackoff(message *reliabletopic.Message) (err error) {
	sleepTime := initialBackoff
	for {
		result, err := r.ringBuffer.Add(message, core.OverflowPolicyFail)
		if err != nil {
			return err
		}
		if result != -1 {
			return nil
		}
		time.Sleep(sleepTime)
		sleepTime *= 2
		if sleepTime > maxBackoff {
			sleepTime = maxBackoff
		}
	}
}

func (r *ReliableTopicProxy) addOrOverwrite(message *reliabletopic.Message) (err error) {
	_, err = r.ringBuffer.Add(message, core.OverflowPolicyOverwrite)
	return
}
func (r *ReliableTopicProxy) Ringbuffer() core.Ringbuffer {
	return r.ringBuffer
}

type messageProcessor struct {
	id        string
	sequence  int64
	cancelled atomic.Value
	listener  core.ReliableMessageListener
	proxy     *ReliableTopicProxy
}

func newMessageProcessor(id string, listener core.ReliableMessageListener, proxy *ReliableTopicProxy) *messageProcessor {
	msgProcessor := &messageProcessor{
		id:       id,
		listener: listener,
		proxy:    proxy,
	}
	msgProcessor.cancelled.Store(false)
	initialSeq := listener.RetrieveInitialSequence()
	if initialSeq == -1 {
		// set initial seq as tail sequence + 1 so that we wont be reading the items that were added before the listener
		// is added.
		tailSeq, _ := msgProcessor.proxy.ringBuffer.TailSequence()
		initialSeq = tailSeq + 1
	}
	msgProcessor.sequence = initialSeq
	return msgProcessor
}

func (m *messageProcessor) next() {
	if m.cancelled.Load() == true {
		return
	}
	readResults, err := m.proxy.ringBuffer.ReadMany(m.sequence, 1,
		m.proxy.config.ReadBatchSize(), nil)
	if err == nil {
		m.onResponse(readResults)
	} else {
		m.onFailure(err)
	}
}

func (m *messageProcessor) onFailure(err error) {
	if m.cancelled.Load() == true {
		return
	}
	if strings.Contains(err.Error(), "com.hazelcast.ringbuffer.StaleSequenceException") {
		if m.listener.IsLossTolerant() {
			headSeq, _ := m.proxy.ringBuffer.HeadSequence()
			msg := "Topic " + m.proxy.name + " ran into a stale sequence. Jumping from old sequence " +
				strconv.Itoa(int(m.sequence)) + " " +
				" to new sequence " + strconv.Itoa(int(headSeq))
			log.Println(msg)
			m.sequence = headSeq
			go m.next()
			return
		}

	}
	msg := "Terminating Message Listener: " + m.id + " on topic: " + m.proxy.name + ". Reason: " + err.Error()
	log.Println(msg)
	m.proxy.msgProcessorsMu.Lock()
	m.cancel()
	m.proxy.msgProcessorsMu.Unlock()
}

func (m *messageProcessor) onResponse(readResults core.ReadResultSet) {
	// We process all messages in batch. So we don't release the thread and reschedule ourselves;
	// but we'll process whatever was received in 1 go.
	var i int32
	for ; i < readResults.Size(); i++ {
		item, err := readResults.Get(i)
		if msg, ok := item.(*reliabletopic.Message); ok && err == nil {
			if m.cancelled.Load() == true {
				return
			}
			m.listener.StoreSequence(m.sequence)
			m.process(msg)
		}
		m.sequence++
	}
	go m.next()
}

func (m *messageProcessor) process(message *reliabletopic.Message) {
	m.listener.OnMessage(m.toMessage(message))
}

func (m *messageProcessor) toMessage(message *reliabletopic.Message) core.Message {
	payload, _ := m.proxy.serializationService.ToObject(message.Payload().(*serialization.Data))
	member := m.proxy.client.ClusterService.GetMember(message.PublisherAddress())
	return proto.NewTopicMessage(payload, message.PublishTime(), member)
}

// This method is already called under msgProcessorMu mutex so no need to lock here.
func (m *messageProcessor) cancel() {
	m.cancelled.Store(true)
	delete(m.proxy.msgProcessors, m.id)
}

type defaultReliableTopicMessageListener struct {
	messageListener core.MessageListener
}

func newDefaultReliableTopicMessageListener(msgListener core.MessageListener) *defaultReliableTopicMessageListener {
	return &defaultReliableTopicMessageListener{
		messageListener: msgListener,
	}
}

func (d *defaultReliableTopicMessageListener) OnMessage(message core.Message) {
	d.messageListener.OnMessage(message)
}

func (d *defaultReliableTopicMessageListener) RetrieveInitialSequence() int64 {
	return -1
}

func (d *defaultReliableTopicMessageListener) StoreSequence(sequence int64) {
	// no op
}

func (d *defaultReliableTopicMessageListener) IsLossTolerant() bool {
	return false
}
