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
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x000F00
	ClientLocalBackupListenerCodecRequestMessageType = int32(3840)
	// hex: 0x000F01
	ClientLocalBackupListenerCodecResponseMessageType = int32(3841)

	// hex: 0x000F02
	ClientLocalBackupListenerCodecEventBackupMessageType = int32(3842)

	ClientLocalBackupListenerCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ClientLocalBackupListenerResponseResponseOffset                         = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ClientLocalBackupListenerEventBackupSourceInvocationCorrelationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Adds listener for backup acks

func EncodeClientLocalBackupListenerRequest() *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ClientLocalBackupListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientLocalBackupListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeClientLocalBackupListenerResponse(clientMessage *proto.ClientMessage) internal.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientLocalBackupListenerResponseResponseOffset)
}

func HandleClientLocalBackupListener(clientMessage *proto.ClientMessage, handleBackupEvent func(sourceInvocationCorrelationId int64)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ClientLocalBackupListenerCodecEventBackupMessageType {
		initialFrame := frameIterator.Next()
		sourceInvocationCorrelationId := FixSizedTypesCodec.DecodeLong(initialFrame.Content, ClientLocalBackupListenerEventBackupSourceInvocationCorrelationIdOffset)
		handleBackupEvent(sourceInvocationCorrelationId)
		return
	}
}
