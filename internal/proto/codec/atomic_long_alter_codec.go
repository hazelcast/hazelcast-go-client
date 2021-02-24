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
	// hex: 0x090200
	AtomicLongAlterCodecRequestMessageType = int32(590336)
	// hex: 0x090201
	AtomicLongAlterCodecResponseMessageType = int32(590337)

	AtomicLongAlterCodecRequestReturnValueTypeOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicLongAlterCodecRequestInitialFrameSize      = AtomicLongAlterCodecRequestReturnValueTypeOffset + proto.IntSizeInBytes

	AtomicLongAlterResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Alters the currently stored value by applying a function on it.

func EncodeAtomicLongAlterRequest(groupId proto.RaftGroupId, name string, function serialization.Data, returnValueType int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, AtomicLongAlterCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, AtomicLongAlterCodecRequestReturnValueTypeOffset, returnValueType)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongAlterCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	EncodeData(clientMessage, function)

	return clientMessage
}

func DecodeAtomicLongAlterResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, AtomicLongAlterResponseResponseOffset)
}
