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
	// hex: 0x0A0500
	AtomicRefSetCodecRequestMessageType = int32(656640)
	// hex: 0x0A0501
	AtomicRefSetCodecResponseMessageType = int32(656641)

	AtomicRefSetCodecRequestReturnOldValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicRefSetCodecRequestInitialFrameSize     = AtomicRefSetCodecRequestReturnOldValueOffset + proto.BooleanSizeInBytes
)

// Atomically sets the given value

func EncodeAtomicRefSetRequest(groupId proto.RaftGroupId, name string, newValue serialization.Data, returnOldValue bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, AtomicRefSetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, AtomicRefSetCodecRequestReturnOldValueOffset, returnOldValue)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	CodecUtil.EncodeNullable(clientMessage, newValue, EncodeData)

	return clientMessage
}

func DecodeAtomicRefSetResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
