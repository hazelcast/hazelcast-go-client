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
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x000900
	ClientAddDistributedObjectListenerCodecRequestMessageType = int32(2304)
	// hex: 0x000901
	ClientAddDistributedObjectListenerCodecResponseMessageType = int32(2305)

	// hex: 0x000902
	ClientAddDistributedObjectListenerCodecEventDistributedObjectMessageType = int32(2306)
	ClientAddDistributedObjectListenerCodecRequestLocalOnlyOffset            = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAddDistributedObjectListenerCodecRequestInitialFrameSize           = ClientAddDistributedObjectListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ClientAddDistributedObjectListenerResponseResponseOffset             = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ClientAddDistributedObjectListenerEventDistributedObjectSourceOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Adds a distributed object listener to the cluster. This listener will be notified
// when a distributed object is created or destroyed.

func EncodeClientAddDistributedObjectListenerRequest(localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ClientAddDistributedObjectListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeBoolean(initialFrame.Content, ClientAddDistributedObjectListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAddDistributedObjectListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeClientAddDistributedObjectListenerResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeUUID(initialFrame.Content, ClientAddDistributedObjectListenerResponseResponseOffset)
}

func HandleClientAddDistributedObjectListener(clientMessage *proto.ClientMessage, handleDistributedObjectEvent func(name string, serviceName string, eventType string, source types.UUID)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ClientAddDistributedObjectListenerCodecEventDistributedObjectMessageType {
		initialFrame := frameIterator.Next()
		source := DecodeUUID(initialFrame.Content, ClientAddDistributedObjectListenerEventDistributedObjectSourceOffset)
		name := DecodeString(frameIterator)
		serviceName := DecodeString(frameIterator)
		eventType := DecodeString(frameIterator)
		handleDistributedObjectEvent(name, serviceName, eventType, source)
		return
	}
}
