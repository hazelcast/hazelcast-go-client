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
	// hex: 0x013000
	MapExecuteOnAllKeysCodecRequestMessageType = int32(77824)
	// hex: 0x013001
	MapExecuteOnAllKeysCodecResponseMessageType = int32(77825)

	MapExecuteOnAllKeysCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Applies the user defined EntryProcessor to the all entries in the map.Returns the results mapped by each key in the map.

func EncodeMapExecuteOnAllKeysRequest(name string, entryProcessor serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapExecuteOnAllKeysCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapExecuteOnAllKeysCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, entryProcessor)

	return clientMessage
}

func DecodeMapExecuteOnAllKeysResponse(clientMessage *proto.ClientMessage) []proto.Pair {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeEntryListForDataAndData(frameIterator)
}
