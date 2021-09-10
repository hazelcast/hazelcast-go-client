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
	// hex: 0x011600
	MapAddEntryListenerToKeyWithPredicateCodecRequestMessageType = int32(71168)
	// hex: 0x011601
	MapAddEntryListenerToKeyWithPredicateCodecResponseMessageType = int32(71169)

	// hex: 0x011602
	MapAddEntryListenerToKeyWithPredicateCodecEventEntryMessageType = int32(71170)

	MapAddEntryListenerToKeyWithPredicateCodecRequestIncludeValueOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyWithPredicateCodecRequestListenerFlagsOffset = MapAddEntryListenerToKeyWithPredicateCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset     = MapAddEntryListenerToKeyWithPredicateCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyWithPredicateCodecRequestInitialFrameSize    = MapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddEntryListenerToKeyWithPredicateResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset                    = MapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyWithPredicateEventEntryNumberOfAffectedEntriesOffset = MapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
// sub-interface for that event.

func EncodeMapAddEntryListenerToKeyWithPredicateRequest(name string, key serialization.Data, predicate serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapAddEntryListenerToKeyWithPredicateCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateCodecRequestIncludeValueOffset, includeValue)
	EncodeInt(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateCodecRequestListenerFlagsOffset, listenerFlags)
	EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerToKeyWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, predicate)

	return clientMessage
}

func DecodeMapAddEntryListenerToKeyWithPredicateResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateResponseResponseOffset)
}

func HandleMapAddEntryListenerToKeyWithPredicate(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid types.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerToKeyWithPredicateCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset)
		uuid := DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset)
		numberOfAffectedEntries := DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyWithPredicateEventEntryNumberOfAffectedEntriesOffset)
		key := DecodeNullableForData(frameIterator)
		value := DecodeNullableForData(frameIterator)
		oldValue := DecodeNullableForData(frameIterator)
		mergingValue := DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
