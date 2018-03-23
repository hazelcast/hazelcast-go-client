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
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type TopicProxy struct {
	*partitionSpecificProxy
}

func newTopicProxy(client *HazelcastClient, serviceName *string, name *string) (*TopicProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &TopicProxy{parSpecProxy}, nil
}

func (topic *TopicProxy) AddMessageListener(messageListener core.TopicMessageListener) (registrationID *string, err error) {
	request := TopicAddMessageListenerEncodeRequest(topic.name, false)
	eventHandler := topic.createEventHandler(messageListener)

	return topic.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return TopicRemoveMessageListenerEncodeRequest(topic.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return TopicAddMessageListenerDecodeResponse(clientMessage)()
		})

}

func (topic *TopicProxy) RemoveMessageListener(registrationID *string) (removed bool, err error) {
	return topic.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *ClientMessage {
		return TopicRemoveMessageListenerEncodeRequest(topic.name, registrationID)
	})
}

func (topic *TopicProxy) Publish(message interface{}) (err error) {
	messageData, err := topic.validateAndSerialize(message)
	if err != nil {
		return err
	}
	request := TopicPublishEncodeRequest(topic.name, messageData)
	_, err = topic.invoke(request)
	return
}

func (topic *TopicProxy) createEventHandler(messageListener core.TopicMessageListener) func(clientMessage *ClientMessage) {
	return func(message *ClientMessage) {
		TopicAddMessageListenerHandle(message, func(itemData *Data, publishTime int64, uuid *string) {
			member := topic.client.ClusterService.GetMemberByUuid(*uuid)
			item, _ := topic.toObject(itemData)
			itemEvent := NewTopicMessage(item, publishTime, member.(*Member))
			messageListener.OnMessage(itemEvent)
		})
	}
}
