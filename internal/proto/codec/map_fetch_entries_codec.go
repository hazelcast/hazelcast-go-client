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
)

const (
	// hex: 0x013800
	MapFetchEntriesCodecRequestMessageType = int32(79872)
	// hex: 0x013801
	MapFetchEntriesCodecResponseMessageType = int32(79873)

	MapFetchEntriesCodecRequestBatchOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapFetchEntriesCodecRequestInitialFrameSize = MapFetchEntriesCodecRequestBatchOffset + proto.IntSizeInBytes
)

// Fetches specified number of entries from the specified partition starting from specified table index.

func EncodeMapFetchEntriesRequest(name string, iterationPointers []proto.Pair, batch int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapFetchEntriesCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapFetchEntriesCodecRequestBatchOffset, batch)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchEntriesCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListIntegerInteger(clientMessage, iterationPointers)

	return clientMessage
}

func DecodeMapFetchEntriesResponse(clientMessage *proto.ClientMessage) (iterationPointers []proto.Pair, entries []proto.Pair) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	iterationPointers = DecodeEntryListIntegerInteger(frameIterator)
	entries = DecodeEntryListForDataAndData(frameIterator)

	return iterationPointers, entries
}
