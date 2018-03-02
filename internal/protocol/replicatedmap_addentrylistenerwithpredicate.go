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
type ReplicatedMapAddEntryListenerWithPredicateResponseParameters struct {
	Response *string
}

func ReplicatedMapAddEntryListenerWithPredicateCalculateSize(name *string  , predicate *Data  , localOnly bool  ) int {
    // Calculates the request payload size
    dataSize := 0
    dataSize += StringCalculateSize(name)
    dataSize += DataCalculateSize(predicate)
    dataSize += BOOL_SIZE_IN_BYTES
    return dataSize
}

func ReplicatedMapAddEntryListenerWithPredicateEncodeRequest(name *string , predicate *Data , localOnly bool ) *ClientMessage {
    // Encode request into clientMessage
    clientMessage := NewClientMessage(nil,ReplicatedMapAddEntryListenerWithPredicateCalculateSize(name, predicate, localOnly))
    clientMessage.SetMessageType(REPLICATEDMAP_ADDENTRYLISTENERWITHPREDICATE)
    clientMessage.IsRetryable =false
    clientMessage.AppendString(name)
    clientMessage.AppendData(predicate)
    clientMessage.AppendBool(localOnly)
    clientMessage.UpdateFrameLength()
    return clientMessage
}

func ReplicatedMapAddEntryListenerWithPredicateDecodeResponse(clientMessage *ClientMessage) *ReplicatedMapAddEntryListenerWithPredicateResponseParameters {
    // Decode response from client message
    parameters := new(ReplicatedMapAddEntryListenerWithPredicateResponseParameters)
    parameters.Response= clientMessage.ReadString()
    return parameters
}


func ReplicatedMapAddEntryListenerWithPredicateHandle(clientMessage *ClientMessage, handleEventEntry func(*Data,*Data,*Data,*Data,int32,*string,int32)){
    // Event handler
    messageType := clientMessage.MessageType()
    if messageType == EVENT_ENTRY && handleEventEntry != nil {
        var key *Data
        if !clientMessage.ReadBool(){
            key = clientMessage.ReadData()
    }
        var value *Data
        if !clientMessage.ReadBool(){
            value = clientMessage.ReadData()
    }
        var oldValue *Data
        if !clientMessage.ReadBool(){
            oldValue = clientMessage.ReadData()
    }
        var mergingValue *Data
        if !clientMessage.ReadBool(){
            mergingValue = clientMessage.ReadData()
    }
        eventType := clientMessage.ReadInt32()
        uuid := clientMessage.ReadString()
        numberOfAffectedEntries := clientMessage.ReadInt32()
        handleEventEntry(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
    }
}

