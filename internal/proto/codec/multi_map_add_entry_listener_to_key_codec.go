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
	// hex: 0x020D00
	MultiMapAddEntryListenerToKeyCodecRequestMessageType = int32(134400)
	// hex: 0x020D01
	MultiMapAddEntryListenerToKeyCodecResponseMessageType = int32(134401)

	// hex: 0x020D02
	MultiMapAddEntryListenerToKeyCodecEventEntryMessageType = int32(134402)

	MultiMapAddEntryListenerToKeyCodecRequestIncludeValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset    = MultiMapAddEntryListenerToKeyCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MultiMapAddEntryListenerToKeyCodecRequestInitialFrameSize   = MultiMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MultiMapAddEntryListenerToKeyResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MultiMapAddEntryListenerToKeyEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapAddEntryListenerToKeyEventEntryUuidOffset                    = MultiMapAddEntryListenerToKeyEventEntryEventTypeOffset + proto.IntSizeInBytes
	MultiMapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset = MultiMapAddEntryListenerToKeyEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds the specified entry listener for the specified key.The listener will be notified for all
// add/remove/update/evict events for the specified key only.
type multimapAddEntryListenerToKeyCodec struct{}

var MultiMapAddEntryListenerToKeyCodec multimapAddEntryListenerToKeyCodec

func (multimapAddEntryListenerToKeyCodec) EncodeRequest(name string, key serialization.Data, includeValue bool, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MultiMapAddEntryListenerToKeyCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MultiMapAddEntryListenerToKeyCodecRequestIncludeValueOffset, includeValue)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MultiMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (multimapAddEntryListenerToKeyCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MultiMapAddEntryListenerToKeyResponseResponseOffset)
}

func (multimapAddEntryListenerToKeyCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MultiMapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MultiMapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MultiMapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MultiMapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
