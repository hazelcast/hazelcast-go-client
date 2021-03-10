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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x0B0300
	CountDownLatchCountDownCodecRequestMessageType = int32(721664)
	// hex: 0x0B0301
	CountDownLatchCountDownCodecResponseMessageType = int32(721665)

	CountDownLatchCountDownCodecRequestInvocationUidOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	CountDownLatchCountDownCodecRequestExpectedRoundOffset = CountDownLatchCountDownCodecRequestInvocationUidOffset + proto.UuidSizeInBytes
	CountDownLatchCountDownCodecRequestInitialFrameSize    = CountDownLatchCountDownCodecRequestExpectedRoundOffset + proto.IntSizeInBytes
)

// Decrements the count of the latch, releasing all waiting threads if
// the count reaches zero. If the current count is greater than zero, then
// it is decremented. If the new count is zero: All waiting threads are
// re-enabled for thread scheduling purposes, and Countdown owner is set to
// null. If the current count equals zero, then nothing happens.

func EncodeCountDownLatchCountDownRequest(groupId proto.RaftGroupId, name string, invocationUid core.UUID, expectedRound int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, CountDownLatchCountDownCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, CountDownLatchCountDownCodecRequestInvocationUidOffset, invocationUid)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, CountDownLatchCountDownCodecRequestExpectedRoundOffset, expectedRound)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CountDownLatchCountDownCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}
