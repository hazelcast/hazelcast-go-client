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
	// hex: 0x0B0200
	CountDownLatchAwaitCodecRequestMessageType = int32(721408)
	// hex: 0x0B0201
	CountDownLatchAwaitCodecResponseMessageType = int32(721409)

	CountDownLatchAwaitCodecRequestInvocationUidOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	CountDownLatchAwaitCodecRequestTimeoutMsOffset     = CountDownLatchAwaitCodecRequestInvocationUidOffset + proto.UuidSizeInBytes
	CountDownLatchAwaitCodecRequestInitialFrameSize    = CountDownLatchAwaitCodecRequestTimeoutMsOffset + proto.LongSizeInBytes

	CountDownLatchAwaitResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Causes the current thread to wait until the latch has counted down
// to zero, or an exception is thrown, or the specified waiting time
// elapses. If the current count is zero then this method returns
// immediately with the value true. If the current count is greater than
// zero, then the current thread becomes disabled for thread scheduling
// purposes and lies dormant until one of five things happen: the count
// reaches zero due to invocations of the {@code countDown} method, this
// ICountDownLatch instance is destroyed, the countdown owner becomes
// disconnected, some other thread Thread#interrupt interrupts the current
// thread, or the specified waiting time elapses. If the count reaches zero
// then the method returns with the value true. If the current thread has
// its interrupted status set on entry to this method, or is interrupted
// while waiting, then {@code InterruptedException} is thrown
// and the current thread's interrupted status is cleared. If the specified
// waiting time elapses then the value false is returned.  If the time is
// less than or equal to zero, the method will not wait at all.

func EncodeCountDownLatchAwaitRequest(groupId proto.RaftGroupId, name string, invocationUid core.UUID, timeoutMs int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CountDownLatchAwaitCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, CountDownLatchAwaitCodecRequestInvocationUidOffset, invocationUid)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, CountDownLatchAwaitCodecRequestTimeoutMsOffset, timeoutMs)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CountDownLatchAwaitCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeCountDownLatchAwaitResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, CountDownLatchAwaitResponseResponseOffset)
}
