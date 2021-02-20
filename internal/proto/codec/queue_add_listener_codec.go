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

	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x031100
	QueueAddListenerCodecRequestMessageType = int32(200960)
	// hex: 0x031101
	QueueAddListenerCodecResponseMessageType = int32(200961)

	// hex: 0x031102
	QueueAddListenerCodecEventItemMessageType = int32(200962)

	QueueAddListenerCodecRequestIncludeValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueueAddListenerCodecRequestLocalOnlyOffset    = QueueAddListenerCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	QueueAddListenerCodecRequestInitialFrameSize   = QueueAddListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	QueueAddListenerResponseResponseOffset   = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	QueueAddListenerEventItemUuidOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueueAddListenerEventItemEventTypeOffset = QueueAddListenerEventItemUuidOffset + proto.UuidSizeInBytes
)

// Adds an listener for this collection. Listener will be notified or all collection add/remove events.
type queueAddListenerCodec struct{}

var QueueAddListenerCodec queueAddListenerCodec

func (queueAddListenerCodec) EncodeRequest(name string, includeValue bool, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueueAddListenerCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, QueueAddListenerCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, QueueAddListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueAddListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (queueAddListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, QueueAddListenerResponseResponseOffset)
}

func (queueAddListenerCodec) Handle(clientMessage *proto.ClientMessage, handleItemEvent func(item serialization.Data, uuid core.UUID, eventType int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == QueueAddListenerCodecEventItemMessageType {
		initialFrame := frameIterator.Next()
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, QueueAddListenerEventItemUuidOffset)
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, QueueAddListenerEventItemEventTypeOffset)
		item := CodecUtil.DecodeNullableForData(frameIterator)
		handleItemEvent(item, uuid, eventType)
		return
	}
}
