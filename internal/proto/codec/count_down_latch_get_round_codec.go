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
	// hex: 0x0B0500
	CountDownLatchGetRoundCodecRequestMessageType = int32(722176)
	// hex: 0x0B0501
	CountDownLatchGetRoundCodecResponseMessageType = int32(722177)

	CountDownLatchGetRoundCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	CountDownLatchGetRoundResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the current round. A round completes when the count value
// reaches to 0 and a new round starts afterwards.
type countdownlatchGetRoundCodec struct{}

var CountDownLatchGetRoundCodec countdownlatchGetRoundCodec

func (countdownlatchGetRoundCodec) EncodeRequest(groupId proto.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CountDownLatchGetRoundCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CountDownLatchGetRoundCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)
	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (countdownlatchGetRoundCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeInt(initialFrame.Content, CountDownLatchGetRoundResponseResponseOffset)
}
