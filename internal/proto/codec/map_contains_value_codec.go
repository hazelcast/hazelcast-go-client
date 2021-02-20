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
	// hex: 0x010700
	MapContainsValueCodecRequestMessageType = int32(67328)
	// hex: 0x010701
	MapContainsValueCodecResponseMessageType = int32(67329)

	MapContainsValueCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	MapContainsValueResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns true if this map maps one or more keys to the specified value.This operation will probably require time
// linear in the map size for most implementations of the Map interface.
type mapContainsValueCodec struct{}

var MapContainsValueCodec mapContainsValueCodec

func (mapContainsValueCodec) EncodeRequest(name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapContainsValueCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapContainsValueCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (mapContainsValueCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapContainsValueResponseResponseOffset)
}
