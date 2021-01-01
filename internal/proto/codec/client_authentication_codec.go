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
type clientAuthenticationCodec struct{}

var ClientAuthenticationCodec clientAuthenticationCodec

func (clientAuthenticationCodec) EncodeRequest(clusterName string, username string, password string, uuid core.UUID, clientType string, serializationVersion byte, clientHazelcastVersion string, clientName string, labels []string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, ClientAuthenticationCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientAuthenticationCodecRequestUuidOffset, uuid)
	internal.FixSizedTypesCodec.EncodeByte(initialFrame.Content, ClientAuthenticationCodecRequestSerializationVersionOffset, serializationVersion)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAuthenticationCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, clusterName)
	internal.CodecUtil.EncodeNullableForString(clientMessage, username)
	internal.CodecUtil.EncodeNullableForString(clientMessage, password)
	internal.StringCodec.Encode(clientMessage, clientType)
	internal.StringCodec.Encode(clientMessage, clientHazelcastVersion)
	internal.StringCodec.Encode(clientMessage, clientName)
	internal.ListMultiFrameCodec.EncodeForString(clientMessage, labels)

	return clientMessage
}

func (clientAuthenticationCodec) DecodeResponse(clientMessage *proto.ClientMessage) (status byte, address core.Address, memberUuid core.UUID, serializationVersion byte, serverHazelcastVersion string, partitionCount int32, clusterId core.UUID, failoverSupported bool) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	status = internal.FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationResponseStatusOffset)
	memberUuid = internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationResponseMemberUuidOffset)
	serializationVersion = internal.FixSizedTypesCodec.DecodeByte(initialFrame.Content, ClientAuthenticationResponseSerializationVersionOffset)
	partitionCount = internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAuthenticationResponsePartitionCountOffset)
	clusterId = internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ClientAuthenticationResponseClusterIdOffset)
	failoverSupported = internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientAuthenticationResponseFailoverSupportedOffset)
	address = internal.CodecUtil.DecodeNullableForAddress(frameIterator)
	serverHazelcastVersion = internal.StringCodec.Decode(frameIterator)

	return status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported
}
