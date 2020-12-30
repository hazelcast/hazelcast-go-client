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
)

const (
	// hex: 0x013D00
	MapFetchNearCacheInvalidationMetadataCodecRequestMessageType = int32(81152)
	// hex: 0x013D01
	MapFetchNearCacheInvalidationMetadataCodecResponseMessageType = int32(81153)

	MapFetchNearCacheInvalidationMetadataCodecRequestUuidOffset       = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapFetchNearCacheInvalidationMetadataCodecRequestInitialFrameSize = MapFetchNearCacheInvalidationMetadataCodecRequestUuidOffset + proto.UuidSizeInBytes
)

// Fetches invalidation metadata from partitions of map.
type mapFetchNearCacheInvalidationMetadataCodec struct{}

var MapFetchNearCacheInvalidationMetadataCodec mapFetchNearCacheInvalidationMetadataCodec

func (mapFetchNearCacheInvalidationMetadataCodec) EncodeRequest(names []string, uuid core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapFetchNearCacheInvalidationMetadataCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MapFetchNearCacheInvalidationMetadataCodecRequestUuidOffset, uuid)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchNearCacheInvalidationMetadataCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.ListMultiFrameCodec.EncodeForString(clientMessage, names)

	return clientMessage
}

func (mapFetchNearCacheInvalidationMetadataCodec) DecodeResponse(clientMessage *proto.ClientMessage) (namePartitionSequenceList []proto.Pair, partitionUuidList []proto.Pair) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	namePartitionSequenceList = internal.EntryListCodec.DecodeForStringAndEntryListIntegerLong(frameIterator)
	partitionUuidList = internal.EntryListIntegerUUIDCodec.Decode(frameIterator)

	return namePartitionSequenceList, partitionUuidList
}
