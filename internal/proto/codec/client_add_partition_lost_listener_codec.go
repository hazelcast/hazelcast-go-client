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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x000600
	ClientAddPartitionLostListenerCodecRequestMessageType = int32(1536)
	// hex: 0x000601
	ClientAddPartitionLostListenerCodecResponseMessageType = int32(1537)

	// hex: 0x000602
	ClientAddPartitionLostListenerCodecEventPartitionLostMessageType = int32(1538)

	ClientAddPartitionLostListenerCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAddPartitionLostListenerCodecRequestInitialFrameSize = ClientAddPartitionLostListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ClientAddPartitionLostListenerResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ClientAddPartitionLostListenerEventPartitionLostPartitionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAddPartitionLostListenerEventPartitionLostLostBackupCountOffset = ClientAddPartitionLostListenerEventPartitionLostPartitionIdOffset + proto.IntSizeInBytes
	ClientAddPartitionLostListenerEventPartitionLostSourceOffset          = ClientAddPartitionLostListenerEventPartitionLostLostBackupCountOffset + proto.IntSizeInBytes
)

// Adds a partition lost listener to the cluster.
type clientAddPartitionLostListenerCodec struct{}

var ClientAddPartitionLostListenerCodec clientAddPartitionLostListenerCodec

func (clientAddPartitionLostListenerCodec) EncodeRequest(localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ClientAddPartitionLostListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ClientAddPartitionLostListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAddPartitionLostListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func (clientAddPartitionLostListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAddPartitionLostListenerResponseResponseOffset)
}

func (clientAddPartitionLostListenerCodec) Handle(clientMessage *proto.ClientMessage, handlePartitionLostEvent func(partitionId int32, lostBackupCount int32, source core.UUID)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ClientAddPartitionLostListenerCodecEventPartitionLostMessageType {
		initialFrame := frameIterator.Next()
		partitionId := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAddPartitionLostListenerEventPartitionLostPartitionIdOffset)
		lostBackupCount := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAddPartitionLostListenerEventPartitionLostLostBackupCountOffset)
		source := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAddPartitionLostListenerEventPartitionLostSourceOffset)
		handlePartitionLostEvent(partitionId, lostBackupCount, source)
		return
	}
}
