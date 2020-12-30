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
	// hex: 0x000200
	ClientAuthenticationCustomCodecRequestMessageType = int32(512)
	// hex: 0x000201
	ClientAuthenticationCustomCodecResponseMessageType = int32(513)

	ClientAuthenticationCustomCodecRequestUuidOffset                 = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAuthenticationCustomCodecRequestSerializationVersionOffset = ClientAuthenticationCustomCodecRequestUuidOffset + proto.UuidSizeInBytes
	ClientAuthenticationCustomCodecRequestInitialFrameSize           = ClientAuthenticationCustomCodecRequestSerializationVersionOffset + proto.ByteSizeInBytes

	ClientAuthenticationCustomResponseStatusOffset               = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ClientAuthenticationCustomResponseMemberUuidOffset           = ClientAuthenticationCustomResponseStatusOffset + proto.ByteSizeInBytes
	ClientAuthenticationCustomResponseSerializationVersionOffset = ClientAuthenticationCustomResponseMemberUuidOffset + proto.UuidSizeInBytes
	ClientAuthenticationCustomResponsePartitionCountOffset       = ClientAuthenticationCustomResponseSerializationVersionOffset + proto.ByteSizeInBytes
	ClientAuthenticationCustomResponseClusterIdOffset            = ClientAuthenticationCustomResponsePartitionCountOffset + proto.IntSizeInBytes
	ClientAuthenticationCustomResponseFailoverSupportedOffset    = ClientAuthenticationCustomResponseClusterIdOffset + proto.UuidSizeInBytes
)

// Makes an authentication request to the cluster using custom credentials.
type clientAuthenticationCustomCodec struct{}

var ClientAuthenticationCustomCodec clientAuthenticationCustomCodec

func (clientAuthenticationCustomCodec) EncodeRequest(clusterName string, credentials []byte, uuid core.UUID, clientType string, serializationVersion byte, clientHazelcastVersion string, clientName string, labels []string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, ClientAuthenticationCustomCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientAuthenticationCustomCodecRequestUuidOffset, uuid)
	internal.FixSizedTypesCodec.EncodeByte(initialFrame.Content, ClientAuthenticationCustomCodecRequestSerializationVersionOffset, serializationVersion)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAuthenticationCustomCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, clusterName)
	internal.ByteArrayCodec.Encode(clientMessage, credentials)
	internal.StringCodec.Encode(clientMessage, clientType)
	internal.StringCodec.Encode(clientMessage, clientHazelcastVersion)
	internal.StringCodec.Encode(clientMessage, clientName)
	internal.ListMultiFrameCodec.EncodeForString(clientMessage, labels)

	return clientMessage
}

func (clientAuthenticationCustomCodec) DecodeResponse(clientMessage *proto.ClientMessage) (status byte, address core.Address, memberUuid core.UUID, serializationVersion byte, serverHazelcastVersion string, partitionCount int32, clusterId core.UUID, failoverSupported bool) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	status = internal.FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationCustomResponseStatusOffset)
	memberUuid = internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationCustomResponseMemberUuidOffset)
	serializationVersion = internal.FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationCustomResponseSerializationVersionOffset)
	partitionCount = internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAuthenticationCustomResponsePartitionCountOffset)
	clusterId = internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationCustomResponseClusterIdOffset)
	failoverSupported = internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientAuthenticationCustomResponseFailoverSupportedOffset)
	address = internal.CodecUtil.DecodeNullableForAddress(frameIterator)
	serverHazelcastVersion = internal.StringCodec.Decode(frameIterator)

	return status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported
}
