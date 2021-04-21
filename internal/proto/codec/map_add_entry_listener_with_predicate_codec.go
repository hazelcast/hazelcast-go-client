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
	"github.com/hazelcast/hazelcast-go-client/serialization"
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
type mapAddEntryListenerWithPredicateCodec struct{}

var MapAddEntryListenerWithPredicateCodec mapAddEntryListenerWithPredicateCodec

func (mapAddEntryListenerWithPredicateCodec) EncodeRequest(name string, predicate serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapAddEntryListenerWithPredicateCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestIncludeValueOffset, includeValue)
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestListenerFlagsOffset, listenerFlags)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, predicate)

	return clientMessage
}

func (mapAddEntryListenerWithPredicateCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerWithPredicateResponseResponseOffset)
}

func (mapAddEntryListenerWithPredicateCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerWithPredicateCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerWithPredicateEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
