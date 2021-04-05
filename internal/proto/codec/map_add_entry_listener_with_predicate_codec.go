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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x011700
	MapAddEntryListenerWithPredicateCodecRequestMessageType = int32(71424)
	// hex: 0x011701
	MapAddEntryListenerWithPredicateCodecResponseMessageType = int32(71425)

	// hex: 0x011702
	MapAddEntryListenerWithPredicateCodecEventEntryMessageType = int32(71426)

	MapAddEntryListenerWithPredicateCodecRequestIncludeValueOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerWithPredicateCodecRequestListenerFlagsOffset = MapAddEntryListenerWithPredicateCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset     = MapAddEntryListenerWithPredicateCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddEntryListenerWithPredicateCodecRequestInitialFrameSize    = MapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddEntryListenerWithPredicateResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddEntryListenerWithPredicateEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerWithPredicateEventEntryUuidOffset                    = MapAddEntryListenerWithPredicateEventEntryEventTypeOffset + proto.IntSizeInBytes
	MapAddEntryListenerWithPredicateEventEntryNumberOfAffectedEntriesOffset = MapAddEntryListenerWithPredicateEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds an continuous entry listener for this map. Listener will get notified for map add/remove/update/evict events
// filtered by the given predicate.

func EncodeMapAddEntryListenerWithPredicateRequest(name string, predicate serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapAddEntryListenerWithPredicateCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestListenerFlagsOffset, listenerFlags)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, predicate)

	return clientMessage
}

func DecodeMapAddEntryListenerWithPredicateResponse(clientMessage *proto.ClientMessage) internal.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerWithPredicateResponseResponseOffset)
}

func HandleMapAddEntryListenerWithPredicate(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid internal.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerWithPredicateCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryEventTypeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryUuidOffset)
		numberOfAffectedEntries := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryNumberOfAffectedEntriesOffset)
		key := CodecUtil.DecodeNullableForData(frameIterator)
		value := CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
