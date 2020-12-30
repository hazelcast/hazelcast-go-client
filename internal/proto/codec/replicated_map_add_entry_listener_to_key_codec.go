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
	// hex: 0x0D0C00
	ReplicatedMapAddEntryListenerToKeyCodecRequestMessageType = int32(855040)
	// hex: 0x0D0C01
	ReplicatedMapAddEntryListenerToKeyCodecResponseMessageType = int32(855041)

	// hex: 0x0D0C02
	ReplicatedMapAddEntryListenerToKeyCodecEventEntryMessageType = int32(855042)

	ReplicatedMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyCodecRequestInitialFrameSize = ReplicatedMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ReplicatedMapAddEntryListenerToKeyResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ReplicatedMapAddEntryListenerToKeyEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyEventEntryUuidOffset                    = ReplicatedMapAddEntryListenerToKeyEventEntryEventTypeOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset = ReplicatedMapAddEntryListenerToKeyEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds the specified entry listener for the specified key. The listener will be notified for all
// add/remove/update/evict events of the specified key only.
type replicatedmapAddEntryListenerToKeyCodec struct{}

var ReplicatedMapAddEntryListenerToKeyCodec replicatedmapAddEntryListenerToKeyCodec

func (replicatedmapAddEntryListenerToKeyCodec) EncodeRequest(name string, key serialization.Data, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ReplicatedMapAddEntryListenerToKeyCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (replicatedmapAddEntryListenerToKeyCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyResponseResponseOffset)
}

func (replicatedmapAddEntryListenerToKeyCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ReplicatedMapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
