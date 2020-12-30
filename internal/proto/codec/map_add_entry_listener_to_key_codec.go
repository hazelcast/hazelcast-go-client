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
type mapAddEntryListenerToKeyCodec struct{}

var MapAddEntryListenerToKeyCodec mapAddEntryListenerToKeyCodec

func (mapAddEntryListenerToKeyCodec) EncodeRequest(name string, key serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapAddEntryListenerToKeyCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestIncludeValueOffset, includeValue)
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset, listenerFlags)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (mapAddEntryListenerToKeyCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyResponseResponseOffset)
}

func (mapAddEntryListenerToKeyCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
