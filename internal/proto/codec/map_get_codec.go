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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x010200
	MapGetCodecRequestMessageType = int32(66048)
	// hex: 0x010201
	MapGetCodecResponseMessageType = int32(66049)

	MapGetCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapGetCodecRequestInitialFrameSize = MapGetCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// This method returns a clone of the original value, so modifying the returned value does not change the actual
// value in the map. You should put the modified value back to make changes visible to all nodes.

func EncodeMapGetRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapGetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapGetCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapGetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapGetResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
