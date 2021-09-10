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
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x011800
	MapAddEntryListenerToKeyCodecRequestMessageType = int32(71680)
	// hex: 0x011801
	MapAddEntryListenerToKeyCodecResponseMessageType = int32(71681)

	// hex: 0x011802
	MapAddEntryListenerToKeyCodecEventEntryMessageType = int32(71682)

	MapAddEntryListenerToKeyCodecRequestIncludeValueOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset = MapAddEntryListenerToKeyCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset     = MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyCodecRequestInitialFrameSize    = MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddEntryListenerToKeyResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddEntryListenerToKeyEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyEventEntryUuidOffset                    = MapAddEntryListenerToKeyEventEntryEventTypeOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset = MapAddEntryListenerToKeyEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
// sub-interface for that event.

func EncodeMapAddEntryListenerToKeyRequest(name string, key serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapAddEntryListenerToKeyCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestIncludeValueOffset, includeValue)
	EncodeInt(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset, listenerFlags)
	EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapAddEntryListenerToKeyResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyResponseResponseOffset)
}

func HandleMapAddEntryListenerToKey(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid types.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := DecodeNullableForData(frameIterator)
		value := DecodeNullableForData(frameIterator)
		oldValue := DecodeNullableForData(frameIterator)
		mergingValue := DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
