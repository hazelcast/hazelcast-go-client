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
)

const (
	// hex: 0x1F0200
	CPSessionCloseSessionCodecRequestMessageType = int32(2032128)
	// hex: 0x1F0201
	CPSessionCloseSessionCodecResponseMessageType = int32(2032129)

	CPSessionCloseSessionCodecRequestSessionIdOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	CPSessionCloseSessionCodecRequestInitialFrameSize = CPSessionCloseSessionCodecRequestSessionIdOffset + proto.LongSizeInBytes

	CPSessionCloseSessionResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Closes the given session on the given CP group

func EncodeCPSessionCloseSessionRequest(groupId proto.RaftGroupId, sessionId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, CPSessionCloseSessionCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, CPSessionCloseSessionCodecRequestSessionIdOffset, sessionId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPSessionCloseSessionCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)

	return clientMessage
}

func DecodeCPSessionCloseSessionResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, CPSessionCloseSessionResponseResponseOffset)
}
