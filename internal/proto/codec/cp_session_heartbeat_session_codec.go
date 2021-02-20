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
	// hex: 0x1F0300
	CPSessionHeartbeatSessionCodecRequestMessageType = int32(2032384)
	// hex: 0x1F0301
	CPSessionHeartbeatSessionCodecResponseMessageType = int32(2032385)

	CPSessionHeartbeatSessionCodecRequestSessionIdOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	CPSessionHeartbeatSessionCodecRequestInitialFrameSize = CPSessionHeartbeatSessionCodecRequestSessionIdOffset + proto.LongSizeInBytes
)

// Commits a heartbeat for the given session on the given cP group and
// extends its session expiration time.
type cpsessionHeartbeatSessionCodec struct{}

var CPSessionHeartbeatSessionCodec cpsessionHeartbeatSessionCodec

func (cpsessionHeartbeatSessionCodec) EncodeRequest(groupId proto.RaftGroupId, sessionId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CPSessionHeartbeatSessionCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, CPSessionHeartbeatSessionCodecRequestSessionIdOffset, sessionId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPSessionHeartbeatSessionCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)

	return clientMessage
}
