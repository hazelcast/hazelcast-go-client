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

type clientAddMembershipListenerCodec struct {
}

func (self *clientAddMembershipListenerCodec) CalculateSize(args ...interface{}) (dataSize int) {
	dataSize += BOOL_SIZE_IN_BYTES
	return
}
func (self *clientAddMembershipListenerCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(CLIENT_ADDMEMBERSHIPLISTENER)
	request.IsRetryable = false
	request.AppendBool(args[0].(bool))
	request.UpdateFrameLength()
	return
}

func (self *clientAddMembershipListenerCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	parameters = clientMessage.ReadString()
	return
}

func (self *clientAddMembershipListenerCodec) Handle(clientMessage *ClientMessage, handleEventMember func(*Member, int32), handleEventMemberList func([]*Member), handleEventMemberAttributeChange func(*string, *string, int32, *string)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_MEMBER && handleEventMember != nil {
		member := MemberCodecDecode(clientMessage)
		eventType := clientMessage.ReadInt32()
		handleEventMember(member, eventType)
	}

	if messageType == EVENT_MEMBERLIST && handleEventMemberList != nil {

		membersSize := clientMessage.ReadInt32()
		members := make([]*Member, membersSize)
		for membersIndex := 0; membersIndex < int(membersSize); membersIndex++ {
			membersItem := MemberCodecDecode(clientMessage)
			members[membersIndex] = membersItem
		}

		handleEventMemberList(members)
	}

	if messageType == EVENT_MEMBERATTRIBUTECHANGE && handleEventMemberAttributeChange != nil {
		uuid := clientMessage.ReadString()
		key := clientMessage.ReadString()
		operationType := clientMessage.ReadInt32()
		var value *string
		if !clientMessage.ReadBool() {
			value = clientMessage.ReadString()
		}
		handleEventMemberAttributeChange(uuid, key, operationType, value)
	}
}
