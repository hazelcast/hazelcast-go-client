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

func ListCompareAndRetainAllCalculateSize(name *string, values []*Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += Int32SizeInBytes
	for _, valuesItem := range values {
		dataSize += DataCalculateSize(valuesItem)
	}
	return dataSize
}

func ListCompareAndRetainAllEncodeRequest(name *string, values []*Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ListCompareAndRetainAllCalculateSize(name, values))
	clientMessage.SetMessageType(LIST_COMPAREANDRETAINALL)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(int32(len(values)))
	for _, valuesItem := range values {
		clientMessage.AppendData(valuesItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ListCompareAndRetainAllDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
