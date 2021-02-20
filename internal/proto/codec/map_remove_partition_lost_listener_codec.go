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
	// hex: 0x011C00
	MapRemovePartitionLostListenerCodecRequestMessageType = int32(72704)
	// hex: 0x011C01
	MapRemovePartitionLostListenerCodecResponseMessageType = int32(72705)

	MapRemovePartitionLostListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapRemovePartitionLostListenerCodecRequestInitialFrameSize     = MapRemovePartitionLostListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	MapRemovePartitionLostListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified map partition lost listener. If there is no such listener added before, this call does no
// change in the cluster and returns false.
type mapRemovePartitionLostListenerCodec struct{}

var MapRemovePartitionLostListenerCodec mapRemovePartitionLostListenerCodec

func (mapRemovePartitionLostListenerCodec) EncodeRequest(name string, registrationId core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapRemovePartitionLostListenerCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MapRemovePartitionLostListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapRemovePartitionLostListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (mapRemovePartitionLostListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapRemovePartitionLostListenerResponseResponseOffset)
}
