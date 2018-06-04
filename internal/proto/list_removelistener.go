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

package proto

import ()

func listRemoveListenerCalculateSize(name string, registrationId string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += stringCalculateSize(registrationId)
	return dataSize
}

// ListRemoveListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ListRemoveListenerEncodeRequest(name string, registrationId string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, listRemoveListenerCalculateSize(name, registrationId))
	clientMessage.SetMessageType(listRemoveListener)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendString(registrationId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// ListRemoveListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ListRemoveListenerDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
