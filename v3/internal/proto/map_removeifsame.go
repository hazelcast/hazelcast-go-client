// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/v3/serialization"

	"github.com/hazelcast/hazelcast-go-client/v3/internal/proto/bufutil"
)

func mapRemoveIfSameCalculateSize(name string, key serialization.Data, value serialization.Data, threadId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(key)
	dataSize += dataCalculateSize(value)
	dataSize += bufutil.Int64SizeInBytes
	return dataSize
}

// MapRemoveIfSameEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapRemoveIfSameEncodeRequest(name string, key serialization.Data, value serialization.Data, threadId int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapRemoveIfSameCalculateSize(name, key, value, threadId))
	clientMessage.SetMessageType(mapRemoveIfSame)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendData(value)
	clientMessage.AppendInt64(threadId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapRemoveIfSameDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapRemoveIfSameDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
