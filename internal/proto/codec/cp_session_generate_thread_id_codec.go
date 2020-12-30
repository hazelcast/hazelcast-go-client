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
	// hex: 0x1F0400
	CPSessionGenerateThreadIdCodecRequestMessageType = int32(2032640)
	// hex: 0x1F0401
	CPSessionGenerateThreadIdCodecResponseMessageType = int32(2032641)

	CPSessionGenerateThreadIdCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	CPSessionGenerateThreadIdResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Generates a new ID for the caller thread. The ID is unique in the given
// CP group.
type cpsessionGenerateThreadIdCodec struct{}

var CPSessionGenerateThreadIdCodec cpsessionGenerateThreadIdCodec

func (cpsessionGenerateThreadIdCodec) EncodeRequest(groupId proto.RaftGroupId) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, CPSessionGenerateThreadIdCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPSessionGenerateThreadIdCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.RaftGroupIdCodec.Encode(clientMessage, groupId)

	return clientMessage
}

func (cpsessionGenerateThreadIdCodec) DecodeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, CPSessionGenerateThreadIdResponseResponseOffset)
}
