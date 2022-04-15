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
	// hex: 0x014700
	MapSetWithMaxIdleCodecRequestMessageType = int32(83712)
	// hex: 0x014701
	MapSetWithMaxIdleCodecResponseMessageType = int32(83713)

	MapSetWithMaxIdleCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapSetWithMaxIdleCodecRequestTtlOffset        = MapSetWithMaxIdleCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapSetWithMaxIdleCodecRequestMaxIdleOffset    = MapSetWithMaxIdleCodecRequestTtlOffset + proto.LongSizeInBytes
	MapSetWithMaxIdleCodecRequestInitialFrameSize = MapSetWithMaxIdleCodecRequestMaxIdleOffset + proto.LongSizeInBytes
)

// Puts an entry into this map with a given ttl (time to live) value and maxIdle.
// Entry will expire and get evicted after the ttl or maxIdle, whichever comes first.
// If ttl and maxIdle are 0, then the entry lives forever.
//
// Similar to the put operation except that set doesn't return the old value, which is more efficient.

func EncodeMapSetWithMaxIdleRequest(name string, key iserialization.Data, value iserialization.Data, threadId int64, ttl int64, maxIdle int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapSetWithMaxIdleCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetWithMaxIdleCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetWithMaxIdleCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetWithMaxIdleCodecRequestMaxIdleOffset, maxIdle)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapSetWithMaxIdleCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapSetWithMaxIdleResponse(clientMessage *proto.ClientMessage) iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
