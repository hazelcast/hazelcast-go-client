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
	// hex: 0x013200
	MapExecuteOnKeysCodecRequestMessageType = int32(78336)
	// hex: 0x013201
	MapExecuteOnKeysCodecResponseMessageType = int32(78337)

	MapExecuteOnKeysCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Applies the user defined EntryProcessor to the entries mapped by the collection of keys.The results mapped by
// each key in the collection.

func EncodeMapExecuteOnKeysRequest(name string, entryProcessor serialization.Data, keys []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapExecuteOnKeysCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapExecuteOnKeysCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, entryProcessor)
	EncodeListMultiFrameForData(clientMessage, keys)

	return clientMessage
}

func DecodeMapExecuteOnKeysResponse(clientMessage *proto.ClientMessage) []proto.Pair {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeEntryListForDataAndData(frameIterator)
}
