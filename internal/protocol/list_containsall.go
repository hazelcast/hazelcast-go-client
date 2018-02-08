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

import (
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"

	. "github.com/hazelcast/hazelcast-go-client/internal/common"
)

type ListContainsAllResponseParameters struct {
	Response bool
}

func ListContainsAllCalculateSize(name *string, values []*Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT_SIZE_IN_BYTES
	for _, valuesItem := range values {
		dataSize += DataCalculateSize(valuesItem)
	}
	return dataSize
}

func ListContainsAllEncodeRequest(name *string, values []*Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ListContainsAllCalculateSize(name, values))
	clientMessage.SetMessageType(LIST_CONTAINSALL)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt(len(values))
	for _, valuesItem := range values {
		clientMessage.AppendData(valuesItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ListContainsAllDecodeResponse(clientMessage *ClientMessage) *ListContainsAllResponseParameters {
	// Decode response from client message
	parameters := new(ListContainsAllResponseParameters)
	parameters.Response = clientMessage.ReadBool()
	return parameters
}
