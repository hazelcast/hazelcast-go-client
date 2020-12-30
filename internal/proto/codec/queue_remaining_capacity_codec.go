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
	// hex: 0x031300
	QueueRemainingCapacityCodecRequestMessageType = int32(201472)
	// hex: 0x031301
	QueueRemainingCapacityCodecResponseMessageType = int32(201473)

	QueueRemainingCapacityCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	QueueRemainingCapacityResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the number of additional elements that this queue can ideally (in the absence of memory or resource
// constraints) accept without blocking, or Integer.MAX_VALUE if there is no intrinsic limit. Note that you cannot
// always tell if an attempt to insert an element will succeed by inspecting remainingCapacity because it may be
// the case that another thread is about to insert or remove an element.
type queueRemainingCapacityCodec struct{}

var QueueRemainingCapacityCodec queueRemainingCapacityCodec

func (queueRemainingCapacityCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueueRemainingCapacityCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueRemainingCapacityCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (queueRemainingCapacityCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, QueueRemainingCapacityResponseResponseOffset)
}
