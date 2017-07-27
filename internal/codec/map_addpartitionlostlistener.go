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
	. "github.com/hazelcast/go-client"
)

type MapAddPartitionLostListenerResponseParameters struct {
	Response string
}

func (codec *MapAddPartitionLostListenerResponseParameters) calculateSize(name string, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func (codec *MapAddPartitionLostListenerResponseParameters) encodeRequest(name string, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, codec.calculateSize(name, localOnly))
	clientMessage.SetMessageType(MAP_ADDPARTITIONLOSTLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func (codec *MapAddPartitionLostListenerResponseParameters) decodeResponse(clientMessage *ClientMessage) *MapAddPartitionLostListenerResponseParameters {
	// Decode response from client message
	parameters := new(MapAddPartitionLostListenerResponseParameters)
	parameters.Response = *clientMessage.ReadString()
	return parameters
}

type HandleMapPartitionLost func(int32, string)

func (codec *MapAddPartitionLostListenerResponseParameters) handle(clientMessage *ClientMessage, handleEventMapPartitionLost HandleMapPartitionLost) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_MAPPARTITIONLOST && handleEventMapPartitionLost != nil {
		partitionId := clientMessage.ReadInt32()
		uuid := *clientMessage.ReadString()
		handleEventMapPartitionLost(partitionId, uuid)
	}
}
