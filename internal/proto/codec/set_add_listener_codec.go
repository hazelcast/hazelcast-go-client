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
	// hex: 0x060B00
	SetAddListenerCodecRequestMessageType = int32(396032)
	// hex: 0x060B01
	SetAddListenerCodecResponseMessageType = int32(396033)

	// hex: 0x060B02
	SetAddListenerCodecEventItemMessageType = int32(396034)

	SetAddListenerCodecRequestIncludeValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	SetAddListenerCodecRequestLocalOnlyOffset    = SetAddListenerCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	SetAddListenerCodecRequestInitialFrameSize   = SetAddListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	SetAddListenerResponseResponseOffset   = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	SetAddListenerEventItemUuidOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	SetAddListenerEventItemEventTypeOffset = SetAddListenerEventItemUuidOffset + proto.UuidSizeInBytes
)

// Adds an item listener for this collection. Listener will be notified for all collection add/remove events.
type setAddListenerCodec struct{}

var SetAddListenerCodec setAddListenerCodec

func (setAddListenerCodec) EncodeRequest(name string, includeValue bool, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, SetAddListenerCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, SetAddListenerCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, SetAddListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetAddListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (setAddListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, SetAddListenerResponseResponseOffset)
}

func (setAddListenerCodec) Handle(clientMessage *proto.ClientMessage, handleItemEvent func(item serialization.Data, uuid core.UUID, eventType int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == SetAddListenerCodecEventItemMessageType {
		initialFrame := frameIterator.Next()
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, SetAddListenerEventItemUuidOffset)
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, SetAddListenerEventItemEventTypeOffset)
		item := CodecUtil.DecodeNullableForData(frameIterator)
		handleItemEvent(item, uuid, eventType)
		return
	}
}
