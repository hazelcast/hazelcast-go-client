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

import ()

func QueueRemoveListenerCalculateSize(name *string, registrationId *string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += StringCalculateSize(registrationId)
	return dataSize
}

func QueueRemoveListenerEncodeRequest(name *string, registrationId *string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, QueueRemoveListenerCalculateSize(name, registrationId))
	clientMessage.SetMessageType(QUEUE_REMOVELISTENER)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendString(registrationId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func QueueRemoveListenerDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
