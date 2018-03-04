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
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"

	. "github.com/hazelcast/hazelcast-go-client/internal/common"
)

func SetAddListenerCalculateSize(name *string, includeValue bool, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func SetAddListenerEncodeRequest(name *string, includeValue bool, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, SetAddListenerCalculateSize(name, includeValue, localOnly))
	clientMessage.SetMessageType(SET_ADDLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendBool(includeValue)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func SetAddListenerDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type SetAddListenerHandleEventItemFunc func(*Data, *string, int32)

func SetAddListenerEventItemDecode(clientMessage *ClientMessage) (item *Data, uuid *string, eventType int32) {

	if !clientMessage.ReadBool() {
		item = clientMessage.ReadData()
	}
	uuid = clientMessage.ReadString()
	eventType = clientMessage.ReadInt32()
	return
}

func SetAddListenerHandle(clientMessage *ClientMessage,
	handleEventItem SetAddListenerHandleEventItemFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_ITEM && handleEventItem != nil {
		handleEventItem(SetAddListenerEventItemDecode(clientMessage))
	}
}
