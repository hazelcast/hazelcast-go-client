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

func EncodeReplicatedMapAddEntryListenerToKeyRequest(name string, key serialization.Data, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ReplicatedMapAddEntryListenerToKeyCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeReplicatedMapAddEntryListenerToKeyResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyResponseResponseOffset)
}

func HandleReplicatedMapAddEntryListenerToKey(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ReplicatedMapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := CodecUtil.DecodeNullableForData(frameIterator)
		value := CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
