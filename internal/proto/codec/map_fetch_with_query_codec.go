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
	// hex: 0x014000
	MapFetchWithQueryCodecRequestMessageType = int32(81920)
	// hex: 0x014001
	MapFetchWithQueryCodecResponseMessageType = int32(81921)

	MapFetchWithQueryCodecRequestBatchOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapFetchWithQueryCodecRequestInitialFrameSize = MapFetchWithQueryCodecRequestBatchOffset + proto.IntSizeInBytes
)

// Fetches the specified number of entries from the specified partition starting from specified table index
// that match the predicate and applies the projection logic on them.

func EncodeMapFetchWithQueryRequest(name string, iterationPointers []proto.Pair, batch int32, projection serialization.Data, predicate serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapFetchWithQueryCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapFetchWithQueryCodecRequestBatchOffset, batch)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchWithQueryCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListIntegerInteger(clientMessage, iterationPointers)
	EncodeData(clientMessage, projection)
	EncodeData(clientMessage, predicate)

	return clientMessage
}

func (mapFetchWithQueryCodec) DecodeResponse(clientMessage *proto.ClientMessage) (results []serialization.Data, iterationPointers []proto.Pair) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	results = DecodeListMultiFrameForDataContainsNullable(frameIterator)
	iterationPointers = DecodeEntryListIntegerInteger(frameIterator)

	return results, iterationPointers
}
