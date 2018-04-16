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
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func MultiMapValueCountCalculateSize(name *string, key *serialization.Data, threadId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(key)
	dataSize += common.Int64SizeInBytes
	return dataSize
}

func MultiMapValueCountEncodeRequest(name *string, key *serialization.Data, threadId int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MultiMapValueCountCalculateSize(name, key, threadId))
	clientMessage.SetMessageType(multimapValueCount)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendInt64(threadId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MultiMapValueCountDecodeResponse(clientMessage *ClientMessage) func() (response int32) {
	// Decode response from client message
	return func() (response int32) {
		response = clientMessage.ReadInt32()
		return
	}
}
