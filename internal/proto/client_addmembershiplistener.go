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
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

func clientAddMembershipListenerCalculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// ClientAddMembershipListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ClientAddMembershipListenerEncodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// ClientAddMembershipListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ClientAddMembershipListenerDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	return func() (response string) {
		//TODO
		//response = clientMessage.ReadString()
		return
	}
}

// ClientAddMembershipListenerHandleEventMemberFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberFunc func(pubcluster.Member, int32)

// ClientAddMembershipListenerHandleEventMemberListFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberListFunc func([]pubcluster.Member)

// ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc func(string, string, int32, string)

// ClientAddMembershipListenerEventMemberDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberDecode(clientMessage *ClientMessage) (
	member pubcluster.Member, eventType int32) {
	//TODO
	return nil, 0
}

// ClientAddMembershipListenerEventMemberListDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberListDecode(clientMessage *ClientMessage) (
	members []pubcluster.Member) {
	//TODO
	return nil

}

// ClientAddMembershipListenerEventMemberAttributeChangeDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage *ClientMessage) (
	uuid string, key string, operationType int32, value string) {
	//TODO
	return "nil", "nil", 0, "nil"
}

// ClientAddMembershipListenerHandle handles the event with the given
// event handler function.
func ClientAddMembershipListenerHandle(clientMessage *ClientMessage,
	handleEventMember ClientAddMembershipListenerHandleEventMemberFunc,
	handleEventMemberList ClientAddMembershipListenerHandleEventMemberListFunc,
	handleEventMemberAttributeChange ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc) {
	//TODO
	// Event handler
	messageType := clientMessage.GetMessageType()
	if messageType == bufutil.EventMember && handleEventMember != nil {
		handleEventMember(ClientAddMembershipListenerEventMemberDecode(clientMessage))
	}
	if messageType == bufutil.EventMemberList && handleEventMemberList != nil {
		handleEventMemberList(ClientAddMembershipListenerEventMemberListDecode(clientMessage))
	}
	if messageType == bufutil.EventMemberAttributeChange && handleEventMemberAttributeChange != nil {
		handleEventMemberAttributeChange(ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage))
	}
}
