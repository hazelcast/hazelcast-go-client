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
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	// hex: 0x014400
	MapPutWithMaxIdleCodecRequestMessageType = int32(82944)
	// hex: 0x014401
	MapPutWithMaxIdleCodecResponseMessageType = int32(82945)

	MapPutWithMaxIdleCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutWithMaxIdleCodecRequestTtlOffset        = MapPutWithMaxIdleCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapPutWithMaxIdleCodecRequestMaxIdleOffset    = MapPutWithMaxIdleCodecRequestTtlOffset + proto.LongSizeInBytes
	MapPutWithMaxIdleCodecRequestInitialFrameSize = MapPutWithMaxIdleCodecRequestMaxIdleOffset + proto.LongSizeInBytes
)

// Puts an entry into this map with a given ttl (time to live) value.Entry will expire and get evicted after the ttl
// If ttl is 0, then the entry lives forever.This method returns a clone of the previous value, not the original
// (identically equal) value previously put into the map.Time resolution for TTL is seconds. The given TTL value is
// rounded to the next closest second value.

func EncodeMapPutWithMaxIdleRequest(name string, key *iserialization.Data, value *iserialization.Data, threadId int64, ttl int64, maxIdle int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapPutWithMaxIdleCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutWithMaxIdleCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutWithMaxIdleCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutWithMaxIdleCodecRequestMaxIdleOffset, maxIdle)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutWithMaxIdleCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapPutWithMaxIdleResponse(clientMessage *proto.ClientMessage) *iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
