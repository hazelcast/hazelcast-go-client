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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/v3/serialization"

	"github.com/hazelcast/hazelcast-go-client/v3/internal/proto/bufutil"
)

func clientAuthenticationCustomCalculateSize(credentials serialization.Data, uuid string, ownerUuid string, isOwnerConnection bool, clientType string, serializationVersion uint8, clientHazelcastVersion string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += dataCalculateSize(credentials)
	dataSize += bufutil.BoolSizeInBytes
	if uuid != "" {
		dataSize += stringCalculateSize(uuid)
	}
	dataSize += bufutil.BoolSizeInBytes
	if ownerUuid != "" {
		dataSize += stringCalculateSize(ownerUuid)
	}
	dataSize += bufutil.BoolSizeInBytes
	dataSize += stringCalculateSize(clientType)
	dataSize += bufutil.Uint8SizeInBytes
	dataSize += stringCalculateSize(clientHazelcastVersion)
	return dataSize
}

// ClientAuthenticationCustomEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ClientAuthenticationCustomEncodeRequest(credentials serialization.Data, uuid string, ownerUuid string, isOwnerConnection bool, clientType string, serializationVersion uint8, clientHazelcastVersion string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, clientAuthenticationCustomCalculateSize(credentials, uuid, ownerUuid, isOwnerConnection, clientType, serializationVersion, clientHazelcastVersion))
	clientMessage.SetMessageType(clientAuthenticationCustom)
	clientMessage.IsRetryable = true
	clientMessage.AppendData(credentials)
	clientMessage.AppendBool(uuid == "")
	if uuid != "" {
		clientMessage.AppendString(uuid)
	}
	clientMessage.AppendBool(ownerUuid == "")
	if ownerUuid != "" {
		clientMessage.AppendString(ownerUuid)
	}
	clientMessage.AppendBool(isOwnerConnection)
	clientMessage.AppendString(clientType)
	clientMessage.AppendUint8(serializationVersion)
	clientMessage.AppendString(clientHazelcastVersion)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// ClientAuthenticationCustomDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ClientAuthenticationCustomDecodeResponse(clientMessage *ClientMessage) func() (status uint8, address *Address, uuid string, ownerUuid string, serializationVersion uint8, serverHazelcastVersion string, clientUnregisteredMembers []*Member) {
	// Decode response from client message
	return func() (status uint8, address *Address, uuid string, ownerUuid string, serializationVersion uint8, serverHazelcastVersion string, clientUnregisteredMembers []*Member) {
		status = clientMessage.ReadUint8()

		if !clientMessage.ReadBool() {
			address = AddressCodecDecode(clientMessage)
		}

		if !clientMessage.ReadBool() {
			uuid = clientMessage.ReadString()
		}

		if !clientMessage.ReadBool() {
			ownerUuid = clientMessage.ReadString()
		}
		serializationVersion = clientMessage.ReadUint8()
		if clientMessage.IsComplete() {
			return
		}
		serverHazelcastVersion = clientMessage.ReadString()

		if !clientMessage.ReadBool() {
			clientUnregisteredMembersSize := clientMessage.ReadInt32()
			clientUnregisteredMembers = make([]*Member, clientUnregisteredMembersSize)
			for clientUnregisteredMembersIndex := 0; clientUnregisteredMembersIndex < int(clientUnregisteredMembersSize); clientUnregisteredMembersIndex++ {
				clientUnregisteredMembersItem := MemberCodecDecode(clientMessage)
				clientUnregisteredMembers[clientUnregisteredMembersIndex] = clientUnregisteredMembersItem
			}
		}
		return
	}
}
