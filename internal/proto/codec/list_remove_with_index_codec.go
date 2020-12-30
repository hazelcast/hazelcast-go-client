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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x051200
	ListRemoveWithIndexCodecRequestMessageType = int32(332288)
	// hex: 0x051201
	ListRemoveWithIndexCodecResponseMessageType = int32(332289)

	ListRemoveWithIndexCodecRequestIndexOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListRemoveWithIndexCodecRequestInitialFrameSize = ListRemoveWithIndexCodecRequestIndexOffset + proto.IntSizeInBytes
)

// Removes the element at the specified position in this list (optional operation). Shifts any subsequent elements
// to the left (subtracts one from their indices). Returns the element that was removed from the list.
type listRemoveWithIndexCodec struct{}

var ListRemoveWithIndexCodec listRemoveWithIndexCodec

func (listRemoveWithIndexCodec) EncodeRequest(name string, index int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ListRemoveWithIndexCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListRemoveWithIndexCodecRequestIndexOffset, index)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListRemoveWithIndexCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (listRemoveWithIndexCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}
