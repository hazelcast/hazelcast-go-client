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

func ListAddWithIndexCalculateSize(name *string, index int32, value *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += DataCalculateSize(value)
	return dataSize
}

func ListAddWithIndexEncodeRequest(name *string, index int32, value *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ListAddWithIndexCalculateSize(name, index, value))
	clientMessage.SetMessageType(LIST_ADDWITHINDEX)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(index)
	clientMessage.AppendData(value)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// Empty decodeResponse(clientMessage), this message has no parameters to decode
