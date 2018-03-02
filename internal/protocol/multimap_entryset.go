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

type MultiMapEntrySetResponseParameters struct {
	Response *[]Pair
}

func MultiMapEntrySetCalculateSize(name *string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	return dataSize
}

func MultiMapEntrySetEncodeRequest(name *string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MultiMapEntrySetCalculateSize(name))
	clientMessage.SetMessageType(MULTIMAP_ENTRYSET)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MultiMapEntrySetDecodeResponse(clientMessage *ClientMessage) *MultiMapEntrySetResponseParameters {
	// Decode response from client message
	parameters := new(MultiMapEntrySetResponseParameters)

	responseSize := clientMessage.ReadInt32()
	response := make([]Pair, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		var responseItem *Pair
		responseItemKey := clientMessage.ReadData()
		responseItemVal := clientMessage.ReadData()
		responseItem.key = responseItemKey
		responseItem.value = responseItemVal
		response[responseIndex] = *responseItem
	}
	parameters.Response = &response

	return parameters
}
