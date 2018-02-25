// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package protocol

import (
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
)

type clientAuthenticationCodec struct {
}

type ClientAuthenticationResponseParameters struct {
	Status                    uint8
	Address                   *Address
	Uuid                      *string
	OwnerUuid                 *string
	SerializationVersion      uint8
	ServerHazelcastVersion    *string
	ClientUnregisteredMembers []*Member
}

func (self *clientAuthenticationCodec) CalculateSize(args ...interface{}) (dataSize int) {
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += StringCalculateSize(args[1].(*string))
	dataSize += BOOL_SIZE_IN_BYTES
	if args[2].(*string) != nil {
		dataSize += StringCalculateSize(args[2].(*string))
	}
	dataSize += BOOL_SIZE_IN_BYTES
	if args[3].(*string) != nil {
		dataSize += StringCalculateSize(args[3].(*string))
	}
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += StringCalculateSize(args[5].(*string))
	dataSize += UINT8_SIZE_IN_BYTES
	dataSize += StringCalculateSize(args[7].(*string))
	return
}
func (self *clientAuthenticationCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(CLIENT_AUTHENTICATION)
	request.IsRetryable = true
	request.AppendString(args[0].(*string))
	request.AppendString(args[1].(*string))
	request.AppendBool(args[2].(*string) == nil)
	if args[2].(*string) != nil {
		request.AppendString(args[2].(*string))
	}
	request.AppendBool(args[3].(*string) == nil)
	if args[3].(*string) != nil {
		request.AppendString(args[3].(*string))
	}
	request.AppendBool(args[4].(bool))
	request.AppendString(args[5].(*string))
	request.AppendUint8(args[6].(uint8))
	request.AppendString(args[7].(*string))
	request.UpdateFrameLength()
	return
}

func (self *clientAuthenticationCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	response := new(ClientAuthenticationResponseParameters)
	response.Status = clientMessage.ReadUint8()

	if !clientMessage.ReadBool() {
		response.Address = AddressCodecDecode(clientMessage)
	}

	if !clientMessage.ReadBool() {
		response.Uuid = clientMessage.ReadString()
	}

	if !clientMessage.ReadBool() {
		response.OwnerUuid = clientMessage.ReadString()
	}
	response.SerializationVersion = clientMessage.ReadUint8()
	response.ServerHazelcastVersion = clientMessage.ReadString()

	if !clientMessage.ReadBool() {

		clientUnregisteredMembersSize := clientMessage.ReadInt32()
		clientUnregisteredMembers := make([]*Member, clientUnregisteredMembersSize)
		for clientUnregisteredMembersIndex := 0; clientUnregisteredMembersIndex < int(clientUnregisteredMembersSize); clientUnregisteredMembersIndex++ {
			clientUnregisteredMembersItem := MemberCodecDecode(clientMessage)
			clientUnregisteredMembers[clientUnregisteredMembersIndex] = clientUnregisteredMembersItem
		}
		response.ClientUnregisteredMembers = clientUnregisteredMembers

	}
	parameters = response
	return
}
