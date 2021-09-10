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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x013700
	MapFetchKeysCodecRequestMessageType = int32(79616)
	// hex: 0x013701
	MapFetchKeysCodecResponseMessageType = int32(79617)

	MapFetchKeysCodecRequestBatchOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapFetchKeysCodecRequestInitialFrameSize = MapFetchKeysCodecRequestBatchOffset + proto.IntSizeInBytes
)

// Fetches specified number of keys from the specified partition starting from specified table index.

func EncodeMapFetchKeysRequest(name string, iterationPointers []proto.Pair, batch int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapFetchKeysCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeInt(initialFrame.Content, MapFetchKeysCodecRequestBatchOffset, batch)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchKeysCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListIntegerInteger(clientMessage, iterationPointers)

	return clientMessage
}

func DecodeMapFetchKeysResponse(clientMessage *proto.ClientMessage) (iterationPointers []proto.Pair, keys []serialization.Data) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	iterationPointers = DecodeEntryListIntegerInteger(frameIterator)
	keys = DecodeListMultiFrameForData(frameIterator)

	return iterationPointers, keys
}
