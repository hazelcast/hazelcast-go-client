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
	// hex: 0x0B0100
	CountDownLatchTrySetCountCodecRequestMessageType = int32(721152)
	// hex: 0x0B0101
	CountDownLatchTrySetCountCodecResponseMessageType = int32(721153)

	CountDownLatchTrySetCountCodecRequestCountOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	CountDownLatchTrySetCountCodecRequestInitialFrameSize = CountDownLatchTrySetCountCodecRequestCountOffset + proto.IntSizeInBytes

	CountDownLatchTrySetCountResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Sets the count to the given value if the current count is zero.
// If the count is not zero, then this method does nothing
// and returns false
type countdownlatchTrySetCountCodec struct{}

var CountDownLatchTrySetCountCodec countdownlatchTrySetCountCodec

func (countdownlatchTrySetCountCodec) EncodeRequest(groupId proto.RaftGroupId, name string, count int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CountDownLatchTrySetCountCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, CountDownLatchTrySetCountCodecRequestCountOffset, count)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CountDownLatchTrySetCountCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)
	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (countdownlatchTrySetCountCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, CountDownLatchTrySetCountResponseResponseOffset)
}
