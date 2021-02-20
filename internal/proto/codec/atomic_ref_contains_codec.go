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
	// hex: 0x0A0300
	AtomicRefContainsCodecRequestMessageType = int32(656128)
	// hex: 0x0A0301
	AtomicRefContainsCodecResponseMessageType = int32(656129)

	AtomicRefContainsCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	AtomicRefContainsResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Checks if the reference contains the value.
type atomicrefContainsCodec struct{}

var AtomicRefContainsCodec atomicrefContainsCodec

func (atomicrefContainsCodec) EncodeRequest(groupId proto.RaftGroupId, name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, AtomicRefContainsCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefContainsCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)
	StringCodec.Encode(clientMessage, name)
	CodecUtil.EncodeNullable(clientMessage, value, DataCodec.Encode)

	return clientMessage
}

func (atomicrefContainsCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, AtomicRefContainsResponseResponseOffset)
}
