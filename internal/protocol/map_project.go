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
package protocol

import (
	. "github.com/hazelcast/go-client/internal/serialization"
)

type MapProjectResponseParameters struct {
	Response *[]Data
}

func MapProjectCalculateSize(name *string, projection *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(projection)
	return dataSize
}

func MapProjectEncodeRequest(name *string, projection *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapProjectCalculateSize(name, projection))
	clientMessage.SetMessageType(MAP_PROJECT)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(projection)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapProjectDecodeResponse(clientMessage *ClientMessage) *MapProjectResponseParameters {
	// Decode response from client message
	parameters := new(MapProjectResponseParameters)

	responseSize := clientMessage.ReadInt32()
	response := make([]Data, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		responseItem := clientMessage.ReadData()
		response[responseIndex] = *responseItem
	}
	parameters.Response = &response

	return parameters
}
