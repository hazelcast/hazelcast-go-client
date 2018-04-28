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

	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
)

func ReplicatedMapAddEntryListenerToKeyWithPredicateCalculateSize(name *string, key *serialization.Data, predicate *serialization.Data, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(key)
	dataSize += dataCalculateSize(predicate)
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

func ReplicatedMapAddEntryListenerToKeyWithPredicateEncodeRequest(name *string, key *serialization.Data, predicate *serialization.Data, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ReplicatedMapAddEntryListenerToKeyWithPredicateCalculateSize(name, key, predicate, localOnly))
	clientMessage.SetMessageType(replicatedmapAddEntryListenerToKeyWithPredicate)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendData(predicate)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ReplicatedMapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type ReplicatedMapAddEntryListenerToKeyWithPredicateHandleEventEntryFunc func(*serialization.Data, *serialization.Data, *serialization.Data, *serialization.Data, int32, *string, int32)

func ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryDecode(clientMessage *ClientMessage) (key *serialization.Data, value *serialization.Data, oldValue *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {

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

func ReplicatedMapAddEntryListenerToKeyWithPredicateHandle(clientMessage *ClientMessage,
	handleEventEntry ReplicatedMapAddEntryListenerToKeyWithPredicateHandleEventEntryFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == bufutil.EventEntry && handleEventEntry != nil {
		handleEventEntry(ReplicatedMapAddEntryListenerToKeyWithPredicateEventEntryDecode(clientMessage))
	}
}
