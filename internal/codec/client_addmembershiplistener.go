// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	. "github.com/hazelcast/go-client/internal"
)

type ClientAddMembershipListenerResponseParameters struct {
	Response string
}

func ClientAddMembershipListenerCalculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func ClientAddMembershipListenerEncodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientAddMembershipListenerCalculateSize(localOnly))
	clientMessage.SetMessageType(CLIENT_ADDMEMBERSHIPLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientAddMembershipListenerDecodeResponse(clientMessage *ClientMessage) *ClientAddMembershipListenerResponseParameters {
	// Decode response from client message
	parameters := new(ClientAddMembershipListenerResponseParameters)
	parameters.Response = *clientMessage.ReadString()
	return parameters
}

func ClientAddMembershipListenerHandle(clientMessage *ClientMessage, handleEventMember func(Member, int32), handleEventMemberList func([]Member), handleEventMemberAttributeChange func(string, string, int32, string)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_MEMBER && handleEventMember != nil {
		member := *MemberCodecDecode(clientMessage)
		eventType := clientMessage.ReadInt32()
		handleEventMember(member, eventType)
	}

	if messageType == EVENT_MEMBERLIST && handleEventMemberList != nil {

		membersSize := clientMessage.ReadInt32()
		members := make([]Member, membersSize)
		for membersIndex := 0; membersIndex < int(membersSize); membersIndex++ {
			membersItem := *MemberCodecDecode(clientMessage)
			members = append(members, membersItem)
		}

		handleEventMemberList(members)
	}

	if messageType == EVENT_MEMBERATTRIBUTECHANGE && handleEventMemberAttributeChange != nil {
		uuid := *clientMessage.ReadString()
		key := *clientMessage.ReadString()
		operationType := clientMessage.ReadInt32()
		var value string
		if !clientMessage.ReadBool() {
			value = *clientMessage.ReadString()
		}
		handleEventMemberAttributeChange(uuid, key, operationType, value)
	}
}
