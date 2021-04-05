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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x012800
	MapEntriesWithPredicateCodecRequestMessageType = int32(75776)
	// hex: 0x012801
	MapEntriesWithPredicateCodecResponseMessageType = int32(75777)

	MapEntriesWithPredicateCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Queries the map based on the specified predicate and returns the matching entries.Specified predicate
// runs on all members in parallel. The collection is NOT backed by the map, so changes to the map are NOT reflected
// in the collection, and vice-versa. This method is always executed by a distributed query, so it may throw a
// QueryResultSizeExceededException if query result size limit is configured.

func EncodeMapEntriesWithPredicateRequest(name string, predicate serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapEntriesWithPredicateCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapEntriesWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, predicate)

	return clientMessage
}

func DecodeMapEntriesWithPredicateResponse(clientMessage *proto.ClientMessage) []proto.Pair {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeEntryListForDataAndData(frameIterator)
}
