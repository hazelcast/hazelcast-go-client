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
)

const (
	AtomicLongGetCodecRequestMessageType  = int32(0x090500)
	AtomicLongGetCodecResponseMessageType = int32(0x090501)

	AtomicLongGetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	AtomicLongGetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Gets the current value.

func EncodeAtomicLongGetRequest(groupId types.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, AtomicLongGetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongGetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeAtomicLongGetResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, AtomicLongGetResponseResponseOffset)
}
