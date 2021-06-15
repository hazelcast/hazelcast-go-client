/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x000100
	ClientAuthenticationCodecRequestMessageType = int32(256)
	// hex: 0x000101
	ClientAuthenticationCodecResponseMessageType = int32(257)

	ClientAuthenticationCodecRequestUuidOffset                 = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAuthenticationCodecRequestSerializationVersionOffset = ClientAuthenticationCodecRequestUuidOffset + proto.UuidSizeInBytes
	ClientAuthenticationCodecRequestInitialFrameSize           = ClientAuthenticationCodecRequestSerializationVersionOffset + proto.ByteSizeInBytes

	ClientAuthenticationResponseStatusOffset               = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ClientAuthenticationResponseMemberUuidOffset           = ClientAuthenticationResponseStatusOffset + proto.ByteSizeInBytes
	ClientAuthenticationResponseSerializationVersionOffset = ClientAuthenticationResponseMemberUuidOffset + proto.UuidSizeInBytes
	ClientAuthenticationResponsePartitionCountOffset       = ClientAuthenticationResponseSerializationVersionOffset + proto.ByteSizeInBytes
	ClientAuthenticationResponseClusterIdOffset            = ClientAuthenticationResponsePartitionCountOffset + proto.IntSizeInBytes
	ClientAuthenticationResponseFailoverSupportedOffset    = ClientAuthenticationResponseClusterIdOffset + proto.UuidSizeInBytes
)

// Makes an authentication request to the cluster.

func EncodeClientAuthenticationRequest(clusterName string, username string, password string, uuid types.UUID, clientType string, serializationVersion byte, clientHazelcastVersion string, clientName string, labels []string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientAuthenticationCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientAuthenticationCodecRequestUuidOffset, uuid)
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, ClientAuthenticationCodecRequestSerializationVersionOffset, serializationVersion)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAuthenticationCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, clusterName)
	CodecUtil.EncodeNullableForString(clientMessage, username)
	CodecUtil.EncodeNullableForString(clientMessage, password)
	EncodeString(clientMessage, clientType)
	EncodeString(clientMessage, clientHazelcastVersion)
	EncodeString(clientMessage, clientName)
	EncodeListMultiFrameForString(clientMessage, labels)

	return clientMessage
}

func DecodeClientAuthenticationResponse(clientMessage *proto.ClientMessage) (status byte, address *cluster.Address, memberUuid types.UUID, serializationVersion byte, serverHazelcastVersion string, partitionCount int32, clusterId types.UUID, failoverSupported bool) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	status = FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationResponseStatusOffset)
	memberUuid = FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationResponseMemberUuidOffset)
	serializationVersion = FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationResponseSerializationVersionOffset)
	partitionCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAuthenticationResponsePartitionCountOffset)
	clusterId = FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationResponseClusterIdOffset)
	failoverSupported = FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientAuthenticationResponseFailoverSupportedOffset)
	address = CodecUtil.DecodeNullableForAddress(frameIterator)
	serverHazelcastVersion = DecodeString(frameIterator)

	return status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported
}
