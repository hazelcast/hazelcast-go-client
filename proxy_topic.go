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

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
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

func (t *Topic) AddListener(handler TopicMessageHandler) (string, error) {
	subscriptionID := t.subscriptionIDGen.NextID()
	if err := t.addListener(false, subscriptionID, handler); err != nil {
		return "", nil
	}
	return event.FormatSubscriptionID(subscriptionID), nil

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

func (t *Topic) addListener(includeValue bool, subscriptionID int64, handler TopicMessageHandler) error {
	request := codec.EncodeQueueAddListenerRequest(t.name, includeValue, t.config.ClusterConfig.SmartRouting)
	listenerHandler := func(msg *proto.ClientMessage) {
		t.userEventDispatcher.Subscribe(eventMessagePublished, subscriptionID, func(event event.Event) {
			if e, ok := event.(*MessagePublished); ok {
				handler(e)
			} else {
				t.logger.Warnf("cannot cast to MessagePublished event")
			}
		})
	}
	responseDecoder := func(response *proto.ClientMessage) internal.UUID {
		return codec.DecodeMapAddEntryListenerResponse(response)
	}
	makeRemoveMsg := func(subscriptionID internal.UUID) *proto.ClientMessage {
		return codec.EncodeMapRemoveEntryListenerRequest(t.name, subscriptionID)
	}
	return t.listenerBinder.Add(request, subscriptionID, listenerHandler, responseDecoder, makeRemoveMsg)
}
