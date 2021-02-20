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
	// hex: 0x051400
	ListIndexOfCodecRequestMessageType = int32(332800)
	// hex: 0x051401
	ListIndexOfCodecResponseMessageType = int32(332801)

	ListIndexOfCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ListIndexOfResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the index of the first occurrence of the specified element in this list, or -1 if this list does not
// contain the element.
type listIndexOfCodec struct{}

var ListIndexOfCodec listIndexOfCodec

func (listIndexOfCodec) EncodeRequest(name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, ListIndexOfCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListIndexOfCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (listIndexOfCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeInt(initialFrame.Content, ListIndexOfResponseResponseOffset)
}
