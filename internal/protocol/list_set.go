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

func ListSetCalculateSize(name *string, index int32, value *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += Int32SizeInBytes
	dataSize += DataCalculateSize(value)
	return dataSize
}

func ListSetEncodeRequest(name *string, index int32, value *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ListSetCalculateSize(name, index, value))
	clientMessage.SetMessageType(listSet)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(index)
	clientMessage.AppendData(value)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ListSetDecodeResponse(clientMessage *ClientMessage) func() (response *Data) {
	// Decode response from client message
	return func() (response *Data) {

		if !clientMessage.ReadBool() {
			response = clientMessage.ReadData()
		}
		return
	}
}
