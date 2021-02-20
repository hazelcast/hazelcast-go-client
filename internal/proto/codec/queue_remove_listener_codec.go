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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x031200
	QueueRemoveListenerCodecRequestMessageType = int32(201216)
	// hex: 0x031201
	QueueRemoveListenerCodecResponseMessageType = int32(201217)

	QueueRemoveListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueueRemoveListenerCodecRequestInitialFrameSize     = QueueRemoveListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	QueueRemoveListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified item listener. If there is no such listener added before, this call does no change in the
// cluster and returns false.
type queueRemoveListenerCodec struct{}

var QueueRemoveListenerCodec queueRemoveListenerCodec

func (queueRemoveListenerCodec) EncodeRequest(name string, registrationId core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, QueueRemoveListenerCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, QueueRemoveListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueRemoveListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (queueRemoveListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, QueueRemoveListenerResponseResponseOffset)
}
