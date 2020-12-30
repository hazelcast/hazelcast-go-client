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
	// hex: 0x0D0A00
	ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestMessageType = int32(854528)
	// hex: 0x0D0A01
	ReplicatedMapAddEntryListenerToKeyWithPredicateCodecResponseMessageType = int32(854529)

	// hex: 0x0D0A02
	ReplicatedMapAddEntryListenerToKeyWithPredicateCodecEventEntryMessageType = int32(854530)

	ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestInitialFrameSize = ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ReplicatedMapAddEntryListenerToKeyWithPredicateResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset                    = ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryNumberOfAffectedEntriesOffset = ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds an continuous entry listener for this map. The listener will be notified for map add/remove/update/evict
// events filtered by the given predicate.
type replicatedmapAddEntryListenerToKeyWithPredicateCodec struct{}

var ReplicatedMapAddEntryListenerToKeyWithPredicateCodec replicatedmapAddEntryListenerToKeyWithPredicateCodec

func (replicatedmapAddEntryListenerToKeyWithPredicateCodec) EncodeRequest(name string, key serialization.Data, predicate serialization.Data, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapAddEntryListenerToKeyWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)
	internal.DataCodec.Encode(clientMessage, predicate)

	return clientMessage
}

func (replicatedmapAddEntryListenerToKeyWithPredicateCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyWithPredicateResponseResponseOffset)
}

func (replicatedmapAddEntryListenerToKeyWithPredicateCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ReplicatedMapAddEntryListenerToKeyWithPredicateCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
