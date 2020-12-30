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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x013600
	MapEntriesWithPagingPredicateCodecRequestMessageType = int32(79360)
	// hex: 0x013601
	MapEntriesWithPagingPredicateCodecResponseMessageType = int32(79361)

	MapEntriesWithPagingPredicateCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Queries the map based on the specified predicate and returns the matching entries. Specified predicate
// runs on all members in parallel. The collection is NOT backed by the map, so changes to the map are NOT reflected
// in the collection, and vice-versa. This method is always executed by a distributed query, so it may throw a
// QueryResultSizeExceededException if query result size limit is configured.
type mapEntriesWithPagingPredicateCodec struct{}

var MapEntriesWithPagingPredicateCodec mapEntriesWithPagingPredicateCodec

func (mapEntriesWithPagingPredicateCodec) EncodeRequest(name string, predicate proto.PagingPredicateHolder) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapEntriesWithPagingPredicateCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapEntriesWithPagingPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.PagingPredicateHolderCodec.Encode(clientMessage, predicate)

	return clientMessage
}

func (mapEntriesWithPagingPredicateCodec) DecodeResponse(clientMessage *proto.ClientMessage) (response []proto.Pair, anchorDataList proto.AnchorDataListHolder) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	response = internal.EntryListCodec.DecodeForDataAndData(frameIterator)
	anchorDataList = internal.AnchorDataListHolderCodec.Decode(frameIterator)

	return response, anchorDataList
}
