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
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
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

func (tp *TopicProxy) AddMessageListener(messageListener core.TopicMessageListener) (registrationID *string, err error) {
	request := protocol.TopicAddMessageListenerEncodeRequest(tp.name, false)
	eventHandler := tp.createEventHandler(messageListener)

	return tp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.TopicRemoveMessageListenerEncodeRequest(tp.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.TopicAddMessageListenerDecodeResponse(clientMessage)()
		})

}

func (tp *TopicProxy) RemoveMessageListener(registrationID *string) (removed bool, err error) {
	return tp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.TopicRemoveMessageListenerEncodeRequest(tp.name, registrationID)
	})
}

func (tp *TopicProxy) Publish(message interface{}) (err error) {
	messageData, err := tp.validateAndSerialize(message)
	if err != nil {
		return err
	}
	request := protocol.TopicPublishEncodeRequest(tp.name, messageData)
	_, err = tp.invoke(request)
	return
}

func (tp *TopicProxy) createEventHandler(messageListener core.TopicMessageListener) func(clientMessage *protocol.ClientMessage) {
	return func(message *protocol.ClientMessage) {
		protocol.TopicAddMessageListenerHandle(message, func(itemData *serialization.Data, publishTime int64, uuid *string) {
			member := tp.client.ClusterService.GetMemberByUuid(*uuid)
			item, _ := tp.toObject(itemData)
			itemEvent := protocol.NewTopicMessage(item, publishTime, member.(*protocol.Member))
			messageListener.OnMessage(itemEvent)
		})
	}
}
