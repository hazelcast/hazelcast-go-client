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

func MapAddEntryListenerToKeyWithPredicateCalculateSize(name *string, key *Data, predicate *Data, includeValue bool, listenerFlags int32, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(key)
	dataSize += DataCalculateSize(predicate)
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func MapAddEntryListenerToKeyWithPredicateEncodeRequest(name *string, key *Data, predicate *Data, includeValue bool, listenerFlags int32, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapAddEntryListenerToKeyWithPredicateCalculateSize(name, key, predicate, includeValue, listenerFlags, localOnly))
	clientMessage.SetMessageType(MAP_ADDENTRYLISTENERTOKEYWITHPREDICATE)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendData(predicate)
	clientMessage.AppendBool(includeValue)
	clientMessage.AppendInt32(listenerFlags)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type MapAddEntryListenerToKeyWithPredicateHandleEventEntryFunc func(*Data, *Data, *Data, *Data, int32, *string, int32)

func MapAddEntryListenerToKeyWithPredicateEventEntryDecode(clientMessage *ClientMessage) (key *Data, value *Data, oldValue *Data, mergingValue *Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {

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

func MapAddEntryListenerToKeyWithPredicateHandle(clientMessage *ClientMessage,
	handleEventEntry MapAddEntryListenerToKeyWithPredicateHandleEventEntryFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_ENTRY && handleEventEntry != nil {
		handleEventEntry(MapAddEntryListenerToKeyWithPredicateEventEntryDecode(clientMessage))
	}
}
