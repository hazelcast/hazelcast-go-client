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

type ClientRemovePartitionLostListenerResponseParameters struct {
	Response bool
}

func (codec *ClientRemovePartitionLostListenerResponseParameters) calculateSize(registrationId string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&registrationId)
	return dataSize
}

func (codec *ClientRemovePartitionLostListenerResponseParameters) encodeRequest(registrationId string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, codec.calculateSize(registrationId))
	clientMessage.SetMessageType(CLIENT_REMOVEPARTITIONLOSTLISTENER)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(registrationId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func (codec *ClientRemovePartitionLostListenerResponseParameters) decodeResponse(clientMessage *ClientMessage) *ClientRemovePartitionLostListenerResponseParameters {
	// Decode response from client message
	parameters := new(ClientRemovePartitionLostListenerResponseParameters)
	parameters.Response = clientMessage.ReadBool()
	return parameters
}
