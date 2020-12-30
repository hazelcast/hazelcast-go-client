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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x0C0600
	SemaphoreAvailablePermitsCodecRequestMessageType = int32(787968)
	// hex: 0x0C0601
	SemaphoreAvailablePermitsCodecResponseMessageType = int32(787969)

	SemaphoreAvailablePermitsCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SemaphoreAvailablePermitsResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the number of available permits.
type semaphoreAvailablePermitsCodec struct{}

var SemaphoreAvailablePermitsCodec semaphoreAvailablePermitsCodec

func (semaphoreAvailablePermitsCodec) EncodeRequest(groupId proto.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, SemaphoreAvailablePermitsCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SemaphoreAvailablePermitsCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.RaftGroupIdCodec.Encode(clientMessage, groupId)
	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (semaphoreAvailablePermitsCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, SemaphoreAvailablePermitsResponseResponseOffset)
}
