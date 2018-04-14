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
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func MultiMapAddEntryListenerCalculateSize(name *string, includeValue bool, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += common.BoolSizeInBytes
	dataSize += common.BoolSizeInBytes
	return dataSize
}

func MultiMapAddEntryListenerEncodeRequest(name *string, includeValue bool, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MultiMapAddEntryListenerCalculateSize(name, includeValue, localOnly))
	clientMessage.SetMessageType(multimapAddEntryListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendBool(includeValue)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MultiMapAddEntryListenerDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type MultiMapAddEntryListenerHandleEventEntryFunc func(*serialization.Data, *serialization.Data, *serialization.Data, *serialization.Data, int32, *string, int32)

func MultiMapAddEntryListenerEventEntryDecode(clientMessage *ClientMessage) (key *serialization.Data, value *serialization.Data, oldValue *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {

	if !clientMessage.ReadBool() {
		key = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		value = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		oldValue = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		mergingValue = clientMessage.ReadData()
	}
	eventType = clientMessage.ReadInt32()
	uuid = clientMessage.ReadString()
	numberOfAffectedEntries = clientMessage.ReadInt32()
	return
}

func MultiMapAddEntryListenerHandle(clientMessage *ClientMessage,
	handleEventEntry MultiMapAddEntryListenerHandleEventEntryFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == common.EventEntry && handleEventEntry != nil {
		handleEventEntry(MultiMapAddEntryListenerEventEntryDecode(clientMessage))
	}
}
