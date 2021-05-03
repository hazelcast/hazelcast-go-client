/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"time"

	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type Topic struct {
	*proxy
	partitionID int32
}

type TopicMessageHandler func(event *MessagePublished)

func newTopic(p *proxy) (*Topic, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &Topic{proxy: p, partitionID: partitionID}, nil
	}
}

func (t *Topic) AddListener(handler TopicMessageHandler) (internal.UUID, error) {
	return t.addListener(handler)
}

func (t *Topic) Publish(message interface{}) error {
	if messageData, err := t.validateAndSerialize(message); err != nil {
		return err
	} else {
		request := codec.EncodeTopicPublishRequest(t.name, messageData)
		_, err := t.invokeOnPartition(context.Background(), request, t.partitionID)
		return err
	}
}

func (t *Topic) PublishAll(messages ...interface{}) error {
	if messagesData, err := t.validateAndSerializeValues(messages...); err != nil {
		return err
	} else {
		request := codec.EncodeTopicPublishAllRequest(t.name, messagesData)
		_, err := t.invokeOnPartition(context.Background(), request, t.partitionID)
		return err
	}
}

func (t *Topic) RemoveListener(subscriptionID internal.UUID) error {
	return t.listenerBinder.Remove(subscriptionID)
}

func (t *Topic) addListener(handler TopicMessageHandler) (internal.UUID, error) {
	subscriptionID := internal.NewUUID()
	addRequest := codec.EncodeTopicAddMessageListenerRequest(t.name, t.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeTopicRemoveMessageListenerRequest(t.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleTopicAddMessageListener(msg, func(itemData serialization.Data, publishTime int64, uuid internal.UUID) {
			if item, err := t.convertToObject(itemData); err != nil {
				t.logger.Warnf("cannot convert data to Go value")
			} else {
				// TODO: get member from uuid
				handler(newMessagePublished(t.name, item, time.Unix(0, publishTime*1000000), nil))
			}
		})
	}
	err := t.listenerBinder.Add(subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}
