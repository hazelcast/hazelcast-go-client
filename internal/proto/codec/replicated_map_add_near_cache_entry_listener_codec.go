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
	// hex: 0x0D1200
	ReplicatedMapAddNearCacheEntryListenerCodecRequestMessageType = int32(856576)
	// hex: 0x0D1201
	ReplicatedMapAddNearCacheEntryListenerCodecResponseMessageType = int32(856577)

	// hex: 0x0D1202
	ReplicatedMapAddNearCacheEntryListenerCodecEventEntryMessageType = int32(856578)

	ReplicatedMapAddNearCacheEntryListenerCodecRequestIncludeValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddNearCacheEntryListenerCodecRequestLocalOnlyOffset    = ReplicatedMapAddNearCacheEntryListenerCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	ReplicatedMapAddNearCacheEntryListenerCodecRequestInitialFrameSize   = ReplicatedMapAddNearCacheEntryListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ReplicatedMapAddNearCacheEntryListenerResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ReplicatedMapAddNearCacheEntryListenerEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddNearCacheEntryListenerEventEntryUuidOffset                    = ReplicatedMapAddNearCacheEntryListenerEventEntryEventTypeOffset + proto.IntSizeInBytes
	ReplicatedMapAddNearCacheEntryListenerEventEntryNumberOfAffectedEntriesOffset = ReplicatedMapAddNearCacheEntryListenerEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds a near cache entry listener for this map. This listener will be notified when an entry is added/removed/updated/evicted/expired etc. so that the near cache entries can be invalidated.
type replicatedmapAddNearCacheEntryListenerCodec struct{}

var ReplicatedMapAddNearCacheEntryListenerCodec replicatedmapAddNearCacheEntryListenerCodec

func (replicatedmapAddNearCacheEntryListenerCodec) EncodeRequest(name string, includeValue bool, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ReplicatedMapAddNearCacheEntryListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerCodecRequestIncludeValueOffset, includeValue)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapAddNearCacheEntryListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (replicatedmapAddNearCacheEntryListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerResponseResponseOffset)
}

func (replicatedmapAddNearCacheEntryListenerCodec) Handle(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid core.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ReplicatedMapAddNearCacheEntryListenerCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerEventEntryEventTypeOffset)
		uuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerEventEntryUuidOffset)
		numberOfAffectedEntries := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddNearCacheEntryListenerEventEntryNumberOfAffectedEntriesOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		value := internal.CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
