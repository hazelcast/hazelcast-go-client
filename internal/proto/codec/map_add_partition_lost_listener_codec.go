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
	// hex: 0x011B00
	MapAddPartitionLostListenerCodecRequestMessageType = int32(72448)
	// hex: 0x011B01
	MapAddPartitionLostListenerCodecResponseMessageType = int32(72449)

	// hex: 0x011B02
	MapAddPartitionLostListenerCodecEventMapPartitionLostMessageType = int32(72450)

	MapAddPartitionLostListenerCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddPartitionLostListenerCodecRequestInitialFrameSize = MapAddPartitionLostListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddPartitionLostListenerResponseResponseOffset                 = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddPartitionLostListenerEventMapPartitionLostPartitionIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddPartitionLostListenerEventMapPartitionLostUuidOffset        = MapAddPartitionLostListenerEventMapPartitionLostPartitionIdOffset + proto.IntSizeInBytes
)

// Adds a MapPartitionLostListener. The addPartitionLostListener returns a register-id. This id is needed to remove
// the MapPartitionLostListener using the removePartitionLostListener(String) method.
// There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
// IMPORTANT: Please see com.hazelcast.partition.PartitionLostListener for weaknesses.
// IMPORTANT: Listeners registered from HazelcastClient may miss some of the map partition lost events due
// to design limitations.

func EncodeMapAddPartitionLostListenerRequest(name string, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapAddPartitionLostListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddPartitionLostListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddPartitionLostListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeMapAddPartitionLostListenerResponse(clientMessage *proto.ClientMessage) internal.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddPartitionLostListenerResponseResponseOffset)
}

func HandleMapAddPartitionLostListener(clientMessage *proto.ClientMessage, handleMapPartitionLostEvent func(partitionId int32, uuid internal.UUID)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddPartitionLostListenerCodecEventMapPartitionLostMessageType {
		initialFrame := frameIterator.Next()
		partitionId := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddPartitionLostListenerEventMapPartitionLostPartitionIdOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddPartitionLostListenerEventMapPartitionLostUuidOffset)
		handleMapPartitionLostEvent(partitionId, uuid)
		return
	}
}
