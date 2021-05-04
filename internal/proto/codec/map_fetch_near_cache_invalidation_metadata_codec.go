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
	"github.com/hazelcast/hazelcast-go-client/types"
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

func EncodeMapFetchNearCacheInvalidationMetadataRequest(names []string, uuid types.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapFetchNearCacheInvalidationMetadataCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MapFetchNearCacheInvalidationMetadataCodecRequestUuidOffset, uuid)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchNearCacheInvalidationMetadataCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeListMultiFrameForString(clientMessage, names)

	return clientMessage
}

func DecodeMapFetchNearCacheInvalidationMetadataResponse(clientMessage *proto.ClientMessage) (namePartitionSequenceList []proto.Pair, partitionUuidList []proto.Pair) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	namePartitionSequenceList = DecodeEntryListForStringAndEntryListIntegerLong(frameIterator)
	partitionUuidList = DecodeEntryListIntegerUUID(frameIterator)

	return namePartitionSequenceList, partitionUuidList
}
