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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x014600
	MapPutIfAbsentWithMaxIdleCodecRequestMessageType = int32(83456)
	// hex: 0x014601
	MapPutIfAbsentWithMaxIdleCodecResponseMessageType = int32(83457)

	MapPutIfAbsentWithMaxIdleCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutIfAbsentWithMaxIdleCodecRequestTtlOffset        = MapPutIfAbsentWithMaxIdleCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapPutIfAbsentWithMaxIdleCodecRequestMaxIdleOffset    = MapPutIfAbsentWithMaxIdleCodecRequestTtlOffset + proto.LongSizeInBytes
	MapPutIfAbsentWithMaxIdleCodecRequestInitialFrameSize = MapPutIfAbsentWithMaxIdleCodecRequestMaxIdleOffset + proto.LongSizeInBytes
)

// Puts an entry into this map with a given ttl (time to live) value if the specified key is not already associated
// with a value. Entry will expire and get evicted after the ttl or maxIdle, whichever comes first.

func EncodeMapPutIfAbsentWithMaxIdleRequest(name string, key serialization.Data, value serialization.Data, threadId int64, ttl int64, maxIdle int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapPutIfAbsentWithMaxIdleCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutIfAbsentWithMaxIdleCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutIfAbsentWithMaxIdleCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutIfAbsentWithMaxIdleCodecRequestMaxIdleOffset, maxIdle)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutIfAbsentWithMaxIdleCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapPutIfAbsentWithMaxIdleResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
