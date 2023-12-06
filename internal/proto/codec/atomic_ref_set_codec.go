/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	AtomicRefSetCodecRequestMessageType  = int32(0x0A0500)
	AtomicRefSetCodecResponseMessageType = int32(0x0A0501)

	AtomicRefSetCodecRequestReturnOldValueOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicRefSetCodecRequestInitialFrameSize     = AtomicRefSetCodecRequestReturnOldValueOffset + proto.BooleanSizeInBytes
)

// Atomically sets the given value

func EncodeAtomicRefSetRequest(groupId types.RaftGroupID, name string, newValue iserialization.Data, returnOldValue bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, AtomicRefSetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeBoolean(initialFrame.Content, AtomicRefSetCodecRequestReturnOldValueOffset, returnOldValue)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	EncodeNullableData(clientMessage, newValue)

	return clientMessage
}

func DecodeAtomicRefSetResponse(clientMessage *proto.ClientMessage) iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeNullableForData(frameIterator)
}
