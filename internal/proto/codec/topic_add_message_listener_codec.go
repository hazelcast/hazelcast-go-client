// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x040200
	TopicAddMessageListenerCodecRequestMessageType = int32(262656)
	// hex: 0x040201
	TopicAddMessageListenerCodecResponseMessageType = int32(262657)

	// hex: 0x040202
	TopicAddMessageListenerCodecEventTopicMessageType = int32(262658)

	TopicAddMessageListenerCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	TopicAddMessageListenerCodecRequestInitialFrameSize = TopicAddMessageListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	TopicAddMessageListenerResponseResponseOffset      = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	TopicAddMessageListenerEventTopicPublishTimeOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	TopicAddMessageListenerEventTopicUuidOffset        = TopicAddMessageListenerEventTopicPublishTimeOffset + proto.LongSizeInBytes
)

// Subscribes to this topic. When someone publishes a message on this topic. onMessage() function of the given
// MessageListener is called. More than one message listener can be added on one instance.
type topicAddMessageListenerCodec struct{}

var TopicAddMessageListenerCodec topicAddMessageListenerCodec

func (topicAddMessageListenerCodec) EncodeRequest(name string, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, TopicAddMessageListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, TopicAddMessageListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(TopicAddMessageListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (topicAddMessageListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, TopicAddMessageListenerResponseResponseOffset)
}

func (topicAddMessageListenerCodec) Handle(clientMessage *proto.ClientMessage, handleTopicEvent func(item serialization.Data, publishTime int64, uuid core.UUID)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == TopicAddMessageListenerCodecEventTopicMessageType {
		initialFrame := frameIterator.Next()
		publishTime := internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, TopicAddMessageListenerEventTopicPublishTimeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, TopicAddMessageListenerEventTopicUuidOffset)
		item := internal.DataCodec.Decode(frameIterator)
		handleTopicEvent(item, publishTime, uuid)
		return
	}
}
