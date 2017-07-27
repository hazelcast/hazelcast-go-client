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

type ClientAddPartitionLostListenerResponseParameters struct {
	Response string
}

func ClientAddPartitionLostListenerCalculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func ClientAddPartitionLostListenerEncodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientAddPartitionLostListenerCalculateSize(localOnly))
	clientMessage.SetMessageType(CLIENT_ADDPARTITIONLOSTLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientAddPartitionLostListenerDecodeResponse(clientMessage *ClientMessage) *ClientAddPartitionLostListenerResponseParameters {
	// Decode response from client message
	parameters := new(ClientAddPartitionLostListenerResponseParameters)
	parameters.Response = *clientMessage.ReadString()
	return parameters
}

func ClientAddPartitionLostListenerHandle(clientMessage *ClientMessage, handleEventPartitionLost func(int32, int32, Address)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_PARTITIONLOST && handleEventPartitionLost != nil {
		partitionId := clientMessage.ReadInt32()
		lostBackupCount := clientMessage.ReadInt32()
		var source Address
		if !clientMessage.ReadBool() {
			source = *AddressCodecDecode(clientMessage)
		}
		handleEventPartitionLost(partitionId, lostBackupCount, source)
	}
}
