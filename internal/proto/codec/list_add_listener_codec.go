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
	// hex: 0x050B00
	ListAddListenerCodecRequestMessageType = int32(330496)
	// hex: 0x050B01
	ListAddListenerCodecResponseMessageType = int32(330497)

	// hex: 0x050B02
	ListAddListenerCodecEventItemMessageType = int32(330498)

	ListAddListenerCodecRequestIncludeValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListAddListenerCodecRequestLocalOnlyOffset    = ListAddListenerCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	ListAddListenerCodecRequestInitialFrameSize   = ListAddListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ListAddListenerResponseResponseOffset   = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ListAddListenerEventItemUuidOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListAddListenerEventItemEventTypeOffset = ListAddListenerEventItemUuidOffset + proto.UuidSizeInBytes
)

// Adds an item listeners for this collection. Listener will be notified for all collection add/remove events.

func EncodeListAddListenerRequest(name string, includeValue bool, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ListAddListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ListAddListenerCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ListAddListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListAddListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeListAddListenerResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ListAddListenerResponseResponseOffset)
}

func HandleListAddListener(clientMessage *proto.ClientMessage, handleItemEvent func(item iserialization.Data, uuid types.UUID, eventType int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ListAddListenerCodecEventItemMessageType {
		initialFrame := frameIterator.Next()
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ListAddListenerEventItemUuidOffset)
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ListAddListenerEventItemEventTypeOffset)
		item := CodecUtil.DecodeNullableForData(frameIterator)
		handleItemEvent(item, uuid, eventType)
		return
	}
}
