// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

package protocol

type ClientAuthenticationResponseParameters struct {
	Status                    uint8
	Address                   *Address
	Uuid                      *string
	OwnerUuid                 *string
	SerializationVersion      uint8
	ServerHazelcastVersion    *string
	ClientUnregisteredMembers *[]Member
}

func ClientAuthenticationCalculateSize(username *string, password *string, uuid *string, ownerUuid *string, isOwnerConnection bool, clientType *string, serializationVersion uint8) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(username)
	dataSize += StringCalculateSize(password)
	dataSize += BOOL_SIZE_IN_BYTES
	if uuid != nil {
		dataSize += StringCalculateSize(uuid)
	}
	dataSize += BOOL_SIZE_IN_BYTES
	if ownerUuid != nil {
		dataSize += StringCalculateSize(ownerUuid)
	}
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += StringCalculateSize(clientType)
	dataSize += UINT8_SIZE_IN_BYTES
	return dataSize
}

func ClientAuthenticationEncodeRequest(username *string, password *string, uuid *string, ownerUuid *string, isOwnerConnection bool, clientType *string, serializationVersion uint8) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientAuthenticationCalculateSize(username, password, uuid, ownerUuid, isOwnerConnection, clientType, serializationVersion))
	clientMessage.SetMessageType(CLIENT_AUTHENTICATION)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(username)
	clientMessage.AppendString(password)
	clientMessage.AppendBool(uuid == nil)
	if uuid != nil {
		clientMessage.AppendString(uuid)
	}
	clientMessage.AppendBool(ownerUuid == nil)
	if ownerUuid != nil {
		clientMessage.AppendString(ownerUuid)
	}
	clientMessage.AppendBool(isOwnerConnection)
	clientMessage.AppendString(clientType)
	clientMessage.AppendUint8(serializationVersion)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientAuthenticationDecodeResponse(clientMessage *ClientMessage) *ClientAuthenticationResponseParameters {
	// Decode response from client message
	parameters := new(ClientAuthenticationResponseParameters)
	parameters.Status = clientMessage.ReadUint8()

	if !clientMessage.ReadBool() {
		parameters.Address = AddressCodecDecode(clientMessage)
	}

	if !clientMessage.ReadBool() {
		parameters.Uuid = clientMessage.ReadString()
	}

	if !clientMessage.ReadBool() {
		parameters.OwnerUuid = clientMessage.ReadString()
	}
	parameters.SerializationVersion = clientMessage.ReadUint8()
	parameters.ServerHazelcastVersion = clientMessage.ReadString()

	if !clientMessage.ReadBool() {

		clientUnregisteredMembersSize := clientMessage.ReadInt32()
		clientUnregisteredMembers := make([]Member, clientUnregisteredMembersSize)
		for clientUnregisteredMembersIndex := 0; clientUnregisteredMembersIndex < int(clientUnregisteredMembersSize); clientUnregisteredMembersIndex++ {
			clientUnregisteredMembersItem := MemberCodecDecode(clientMessage)
			clientUnregisteredMembers[clientUnregisteredMembersIndex] = *clientUnregisteredMembersItem
		}
		parameters.ClientUnregisteredMembers = &clientUnregisteredMembers

	}
	return parameters
}
