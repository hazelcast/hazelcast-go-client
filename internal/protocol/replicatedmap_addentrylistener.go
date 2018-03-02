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
type ReplicatedMapAddEntryListenerResponseParameters struct {
	Response *string
}

func ReplicatedMapAddEntryListenerCalculateSize(name *string  , localOnly bool  ) int {
    // Calculates the request payload size
    dataSize := 0
    dataSize += StringCalculateSize(name)
    dataSize += BOOL_SIZE_IN_BYTES
    return dataSize
}

func ReplicatedMapAddEntryListenerEncodeRequest(name *string , localOnly bool ) *ClientMessage {
    // Encode request into clientMessage
    clientMessage := NewClientMessage(nil,ReplicatedMapAddEntryListenerCalculateSize(name, localOnly))
    clientMessage.SetMessageType(REPLICATEDMAP_ADDENTRYLISTENER)
    clientMessage.IsRetryable =false
    clientMessage.AppendString(name)
    clientMessage.AppendBool(localOnly)
    clientMessage.UpdateFrameLength()
    return clientMessage
}

func ReplicatedMapAddEntryListenerDecodeResponse(clientMessage *ClientMessage) *ReplicatedMapAddEntryListenerResponseParameters {
    // Decode response from client message
    parameters := new(ReplicatedMapAddEntryListenerResponseParameters)
    parameters.Response= clientMessage.ReadString()
    return parameters
}


func ReplicatedMapAddEntryListenerHandle(clientMessage *ClientMessage, handleEventEntry func(*Data,*Data,*Data,*Data,int32,*string,int32)){
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

