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
	// hex: 0x0A0100
	AtomicRefApplyCodecRequestMessageType = int32(655616)
	// hex: 0x0A0101
	AtomicRefApplyCodecResponseMessageType = int32(655617)

	AtomicRefApplyCodecRequestReturnValueTypeOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicRefApplyCodecRequestAlterOffset           = AtomicRefApplyCodecRequestReturnValueTypeOffset + proto.IntSizeInBytes
	AtomicRefApplyCodecRequestInitialFrameSize      = AtomicRefApplyCodecRequestAlterOffset + proto.BooleanSizeInBytes
)

// Applies a function on the value
type atomicrefApplyCodec struct{}

var AtomicRefApplyCodec atomicrefApplyCodec

func (atomicrefApplyCodec) EncodeRequest(groupId proto.RaftGroupId, name string, function serialization.Data, returnValueType int32, alter bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, AtomicRefApplyCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, AtomicRefApplyCodecRequestReturnValueTypeOffset, returnValueType)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, AtomicRefApplyCodecRequestAlterOffset, alter)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefApplyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.RaftGroupIdCodec.Encode(clientMessage, groupId)
	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, function)

	return clientMessage
}

func (atomicrefApplyCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}
