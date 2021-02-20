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
	// hex: 0x010300
	MapRemoveCodecRequestMessageType = int32(66304)
	// hex: 0x010301
	MapRemoveCodecResponseMessageType = int32(66305)

	MapRemoveCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapRemoveCodecRequestInitialFrameSize = MapRemoveCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Removes the mapping for a key from this map if it is present (optional operation).
// Returns the value to which this map previously associated the key, or null if the map contained no mapping for the key.
// If this map permits null values, then a return value of null does not necessarily indicate that the map contained no mapping for the key; it's also
// possible that the map explicitly mapped the key to null. The map will not contain a mapping for the specified key once the
// call returns.
type mapRemoveCodec struct{}

var MapRemoveCodec mapRemoveCodec

func (mapRemoveCodec) EncodeRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapRemoveCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapRemoveCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapRemoveCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (mapRemoveCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
