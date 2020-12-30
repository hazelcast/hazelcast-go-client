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
)

const (
	// hex: 0x040300
	TopicRemoveMessageListenerCodecRequestMessageType = int32(262912)
	// hex: 0x040301
	TopicRemoveMessageListenerCodecResponseMessageType = int32(262913)

	TopicRemoveMessageListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	TopicRemoveMessageListenerCodecRequestInitialFrameSize     = TopicRemoveMessageListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	TopicRemoveMessageListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Stops receiving messages for the given message listener.If the given listener already removed, this method does nothing.
type topicRemoveMessageListenerCodec struct{}

var TopicRemoveMessageListenerCodec topicRemoveMessageListenerCodec

func (topicRemoveMessageListenerCodec) EncodeRequest(name string, registrationId core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, TopicRemoveMessageListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, TopicRemoveMessageListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(TopicRemoveMessageListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (topicRemoveMessageListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, TopicRemoveMessageListenerResponseResponseOffset)
}
