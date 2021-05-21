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
	// hex: 0x014500
	MapPutTransientWithMaxIdleCodecRequestMessageType = int32(83200)
	// hex: 0x014501
	MapPutTransientWithMaxIdleCodecResponseMessageType = int32(83201)

	MapPutTransientWithMaxIdleCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutTransientWithMaxIdleCodecRequestTtlOffset        = MapPutTransientWithMaxIdleCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapPutTransientWithMaxIdleCodecRequestMaxIdleOffset    = MapPutTransientWithMaxIdleCodecRequestTtlOffset + proto.LongSizeInBytes
	MapPutTransientWithMaxIdleCodecRequestInitialFrameSize = MapPutTransientWithMaxIdleCodecRequestMaxIdleOffset + proto.LongSizeInBytes
)

// Same as put except that MapStore, if defined, will not be called to store/persist the entry.
// If ttl and maxIdle are 0, then the entry lives forever.

func EncodeMapPutTransientWithMaxIdleRequest(name string, key *iserialization.Data, value *iserialization.Data, threadId int64, ttl int64, maxIdle int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapPutTransientWithMaxIdleCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutTransientWithMaxIdleCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutTransientWithMaxIdleCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutTransientWithMaxIdleCodecRequestMaxIdleOffset, maxIdle)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutTransientWithMaxIdleCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapPutTransientWithMaxIdleResponse(clientMessage *proto.ClientMessage) *iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
