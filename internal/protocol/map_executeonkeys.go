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
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type MapExecuteOnKeysResponseParameters struct {
	Response []Pair
}

func MapExecuteOnKeysCalculateSize(name string, entryProcessor Data, keys []Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += DataCalculateSize(&entryProcessor)
	dataSize += INT_SIZE_IN_BYTES
	for _, keysItem := range keys {
		dataSize += DataCalculateSize(&keysItem)
	}
	return dataSize
}

func MapExecuteOnKeysEncodeRequest(name string, entryProcessor Data, keys []Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapExecuteOnKeysCalculateSize(name, entryProcessor, keys))
	clientMessage.SetMessageType(MAP_EXECUTEONKEYS)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(entryProcessor)
	clientMessage.AppendInt(len(keys))
	for _, keysItem := range keys {
		clientMessage.AppendData(keysItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapExecuteOnKeysDecodeResponse(clientMessage *ClientMessage) *MapExecuteOnKeysResponseParameters {
	// Decode response from client message
	parameters := new(MapExecuteOnKeysResponseParameters)

	responseSize := clientMessage.ReadInt32()
	response := make([]Pair, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		var responseItem Pair
		responseItemKey := clientMessage.ReadData()
		responseItemVal := clientMessage.ReadData()
		responseItem.key = responseItemKey
		responseItem.value = responseItemVal
		response = append(response, responseItem)
	}
	parameters.Response = response

	return parameters
}
