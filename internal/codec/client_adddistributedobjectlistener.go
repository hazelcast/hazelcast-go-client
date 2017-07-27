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

type ClientAddDistributedObjectListenerResponseParameters struct {
	Response string
}

func (codec *ClientAddDistributedObjectListenerResponseParameters) calculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func (codec *ClientAddDistributedObjectListenerResponseParameters) encodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, codec.calculateSize(localOnly))
	clientMessage.SetMessageType(CLIENT_ADDDISTRIBUTEDOBJECTLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func (codec *ClientAddDistributedObjectListenerResponseParameters) decodeResponse(clientMessage *ClientMessage) *ClientAddDistributedObjectListenerResponseParameters {
	// Decode response from client message
	parameters := new(ClientAddDistributedObjectListenerResponseParameters)
	parameters.Response = *clientMessage.ReadString()
	return parameters
}

type HandleDistributedObject func(string, string, string)

func (codec *ClientAddDistributedObjectListenerResponseParameters) handle(clientMessage *ClientMessage, handleEventDistributedObject HandleDistributedObject) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_DISTRIBUTEDOBJECT && handleEventDistributedObject != nil {
		name := *clientMessage.ReadString()
		serviceName := *clientMessage.ReadString()
		eventType := *clientMessage.ReadString()
		handleEventDistributedObject(name, serviceName, eventType)
	}
}
