/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x170600
	RingBufferAddCodecRequestMessageType = int32(1508864)
	// hex: 0x170601
	RingBufferAddCodecResponseMessageType = int32(1508865)

	RingBufferAddCodecRequestOverflowPolicyOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	RingBufferAddCodecRequestInitialFrameSize     = RingBufferAddCodecRequestOverflowPolicyOffset + proto.IntSizeInBytes
	RingBufferAddResponseResponseOffset           = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// TODO notram509 add documentation
func EncodeRingBufferAddRequest(name string, overflowPolicy types.OverflowPolicy, value iserialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, RingBufferAddCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, RingBufferAddCodecRequestOverflowPolicyOffset, int32(overflowPolicy))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingBufferAddCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeRingBufferAddResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, RingBufferAddResponseResponseOffset)
}
