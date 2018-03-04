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

func ListAddAllWithIndexCalculateSize(name *string, index int32, valueList []*Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT_SIZE_IN_BYTES
	for _, valueListItem := range valueList {
		dataSize += DataCalculateSize(valueListItem)
	}
	return dataSize
}

func ListAddAllWithIndexEncodeRequest(name *string, index int32, valueList []*Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ListAddAllWithIndexCalculateSize(name, index, valueList))
	clientMessage.SetMessageType(LIST_ADDALLWITHINDEX)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(index)
	clientMessage.AppendInt32(int32(len(valueList)))
	for _, valueListItem := range valueList {
		clientMessage.AppendData(valueListItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ListAddAllWithIndexDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
