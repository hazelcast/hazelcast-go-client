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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func ClientAddMembershipListenerCalculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += common.BoolSizeInBytes
	return dataSize
}

func ClientAddMembershipListenerEncodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientAddMembershipListenerCalculateSize(localOnly))
	clientMessage.SetMessageType(clientAddMembershipListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientAddMembershipListenerDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type ClientAddMembershipListenerHandleEventMemberFunc func(*Member, int32)
type ClientAddMembershipListenerHandleEventMemberListFunc func([]*Member)
type ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc func(*string, *string, int32, *string)

func ClientAddMembershipListenerEventMemberDecode(clientMessage *ClientMessage) (member *Member, eventType int32) {
	member = MemberCodecDecode(clientMessage)
	eventType = clientMessage.ReadInt32()
	return
}
func ClientAddMembershipListenerEventMemberListDecode(clientMessage *ClientMessage) (members []*Member) {
	membersSize := clientMessage.ReadInt32()
	members = make([]*Member, membersSize)
	for membersIndex := 0; membersIndex < int(membersSize); membersIndex++ {
		membersItem := MemberCodecDecode(clientMessage)
		members[membersIndex] = membersItem
	}
	return
}
func ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage *ClientMessage) (uuid *string, key *string, operationType int32, value *string) {
	uuid = clientMessage.ReadString()
	key = clientMessage.ReadString()
	operationType = clientMessage.ReadInt32()

	if !clientMessage.ReadBool() {
		value = clientMessage.ReadString()
	}
	return
}

func ClientAddMembershipListenerHandle(clientMessage *ClientMessage,
	handleEventMember ClientAddMembershipListenerHandleEventMemberFunc,
	handleEventMemberList ClientAddMembershipListenerHandleEventMemberListFunc,
	handleEventMemberAttributeChange ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == common.EventMember && handleEventMember != nil {
		handleEventMember(ClientAddMembershipListenerEventMemberDecode(clientMessage))
	}
	if messageType == common.EventMemberList && handleEventMemberList != nil {
		handleEventMemberList(ClientAddMembershipListenerEventMemberListDecode(clientMessage))
	}
	if messageType == common.EventMemberAttributeChange && handleEventMemberAttributeChange != nil {
		handleEventMemberAttributeChange(ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage))
	}
}
