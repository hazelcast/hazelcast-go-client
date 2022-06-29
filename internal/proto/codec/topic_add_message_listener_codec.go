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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
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

func EncodeTopicAddMessageListenerRequest(name string, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, TopicAddMessageListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, TopicAddMessageListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(TopicAddMessageListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeTopicAddMessageListenerResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, TopicAddMessageListenerResponseResponseOffset)
}

func HandleTopicAddMessageListener(clientMessage *proto.ClientMessage, handleTopicEvent func(item iserialization.Data, publishTime int64, uuid types.UUID)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == TopicAddMessageListenerCodecEventTopicMessageType {
		initialFrame := frameIterator.Next()
		publishTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, TopicAddMessageListenerEventTopicPublishTimeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, TopicAddMessageListenerEventTopicUuidOffset)
		item := DecodeData(frameIterator)
		handleTopicEvent(item, publishTime, uuid)
		return
	}
}
