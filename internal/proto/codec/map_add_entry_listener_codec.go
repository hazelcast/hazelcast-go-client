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
	// hex: 0x011900
	MapAddEntryListenerCodecRequestMessageType = int32(71936)
	// hex: 0x011901
	MapAddEntryListenerCodecResponseMessageType = int32(71937)

	// hex: 0x011902
	MapAddEntryListenerCodecEventEntryMessageType = int32(71938)

	MapAddEntryListenerCodecRequestIncludeValueOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerCodecRequestListenerFlagsOffset = MapAddEntryListenerCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MapAddEntryListenerCodecRequestLocalOnlyOffset     = MapAddEntryListenerCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddEntryListenerCodecRequestInitialFrameSize    = MapAddEntryListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddEntryListenerResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddEntryListenerEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerEventEntryUuidOffset                    = MapAddEntryListenerEventEntryEventTypeOffset + proto.IntSizeInBytes
	MapAddEntryListenerEventEntryNumberOfAffectedEntriesOffset = MapAddEntryListenerEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
// sub-interface for that event.

func EncodeMapAddEntryListenerRequest(name string, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapAddEntryListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddEntryListenerCodecRequestListenerFlagsOffset, listenerFlags)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeMapAddEntryListenerResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerResponseResponseOffset)
}

func HandleMapAddEntryListener(clientMessage *proto.ClientMessage, handleEntryEvent func(key *iserialization.Data, value *iserialization.Data, oldValue *iserialization.Data, mergingValue *iserialization.Data, eventType int32, uuid types.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerEventEntryEventTypeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerEventEntryUuidOffset)
		numberOfAffectedEntries := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerEventEntryNumberOfAffectedEntriesOffset)
		key := CodecUtil.DecodeNullableForData(frameIterator)
		value := CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
