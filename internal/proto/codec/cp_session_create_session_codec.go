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
	// hex: 0x1F0100
	CPSessionCreateSessionCodecRequestMessageType = int32(2031872)
	// hex: 0x1F0101
	CPSessionCreateSessionCodecResponseMessageType = int32(2031873)

	CPSessionCreateSessionCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	CPSessionCreateSessionResponseSessionIdOffset       = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	CPSessionCreateSessionResponseTtlMillisOffset       = CPSessionCreateSessionResponseSessionIdOffset + proto.LongSizeInBytes
	CPSessionCreateSessionResponseHeartbeatMillisOffset = CPSessionCreateSessionResponseTtlMillisOffset + proto.LongSizeInBytes
)

// Creates a session for the caller on the given CP group.

func EncodeCPSessionCreateSessionRequest(groupId proto.RaftGroupId, endpointName string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CPSessionCreateSessionCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPSessionCreateSessionCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, endpointName)

	return clientMessage
}

func (cpsessionCreateSessionCodec) DecodeResponse(clientMessage *proto.ClientMessage) (sessionId int64, ttlMillis int64, heartbeatMillis int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	sessionId = FixSizedTypesCodec.DecodeLong(initialFrame.Content, CPSessionCreateSessionResponseSessionIdOffset)
	ttlMillis = FixSizedTypesCodec.DecodeLong(initialFrame.Content, CPSessionCreateSessionResponseTtlMillisOffset)
	heartbeatMillis = FixSizedTypesCodec.DecodeLong(initialFrame.Content, CPSessionCreateSessionResponseHeartbeatMillisOffset)

	return sessionId, ttlMillis, heartbeatMillis
}
