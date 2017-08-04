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
	. "github.com/hazelcast/go-client/internal"
)

type MapProjectWithPredicateResponseParameters struct {
	Response []Data
}

func MapProjectWithPredicateCalculateSize(name string, projection Data, predicate Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += DataCalculateSize(&projection)
	dataSize += DataCalculateSize(&predicate)
	return dataSize
}

func MapProjectWithPredicateEncodeRequest(name string, projection Data, predicate Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapProjectWithPredicateCalculateSize(name, projection, predicate))
	clientMessage.SetMessageType(MAP_PROJECTWITHPREDICATE)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(projection)
	clientMessage.AppendData(predicate)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapProjectWithPredicateDecodeResponse(clientMessage *ClientMessage) *MapProjectWithPredicateResponseParameters {
	// Decode response from client message
	parameters := new(MapProjectWithPredicateResponseParameters)

	responseSize := clientMessage.ReadInt32()
	response := make([]Data, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		responseItem := clientMessage.ReadData()
		response = append(response, responseItem)
	}
	parameters.Response = response

	return parameters
}
