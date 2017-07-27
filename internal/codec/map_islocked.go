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

type MapIsLockedResponseParameters struct {
	Response bool
}

func (codec *MapIsLockedResponseParameters) calculateSize(name string, key Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += DataCalculateSize(&key)
	return dataSize
}

func (codec *MapIsLockedResponseParameters) encodeRequest(name string, key Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, codec.calculateSize(name, key))
	clientMessage.SetMessageType(MAP_ISLOCKED)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func (codec *MapIsLockedResponseParameters) decodeResponse(clientMessage *ClientMessage) *MapIsLockedResponseParameters {
	// Decode response from client message
	parameters := new(MapIsLockedResponseParameters)
	parameters.Response = clientMessage.ReadBool()
	return parameters
}
