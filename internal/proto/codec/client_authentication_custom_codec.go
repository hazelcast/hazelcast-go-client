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
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
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

func EncodeClientAuthenticationCustomRequest(clusterName string, credentials []byte, uuid internal.UUID, clientType string, serializationVersion byte, clientHazelcastVersion string, clientName string, labels []string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientAuthenticationCustomCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientAuthenticationCustomCodecRequestUuidOffset, uuid)
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, ClientAuthenticationCustomCodecRequestSerializationVersionOffset, serializationVersion)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAuthenticationCustomCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, clusterName)
	EncodeByteArray(clientMessage, credentials)
	EncodeString(clientMessage, clientType)
	EncodeString(clientMessage, clientHazelcastVersion)
	EncodeString(clientMessage, clientName)
	EncodeListMultiFrameForString(clientMessage, labels)

	return clientMessage
}

func DecodeClientAuthenticationCustomResponse(clientMessage *proto.ClientMessage) (status byte, address cluster.Address, memberUuid internal.UUID, serializationVersion byte, serverHazelcastVersion string, partitionCount int32, clusterId internal.UUID, failoverSupported bool) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	status = FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationCustomResponseStatusOffset)
	memberUuid = FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationCustomResponseMemberUuidOffset)
	serializationVersion = FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationCustomResponseSerializationVersionOffset)
	partitionCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAuthenticationCustomResponsePartitionCountOffset)
	clusterId = FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationCustomResponseClusterIdOffset)
	failoverSupported = FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientAuthenticationCustomResponseFailoverSupportedOffset)
	address = CodecUtil.DecodeNullableForAddress(frameIterator)
	serverHazelcastVersion = DecodeString(frameIterator)

	return status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported
}
