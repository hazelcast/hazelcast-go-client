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

	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
)

func RingbufferAddCalculateSize(name string, overflowPolicy int32, value *serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	dataSize += dataCalculateSize(value)
	return dataSize
}

func RingbufferAddEncodeRequest(name string, overflowPolicy int32, value *serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, RingbufferAddCalculateSize(name, overflowPolicy, value))
	clientMessage.SetMessageType(ringbufferAdd)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(overflowPolicy)
	clientMessage.AppendData(value)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func RingbufferAddDecodeResponse(clientMessage *ClientMessage) func() (response int64) {
	// Decode response from client message
	return func() (response int64) {
		response = clientMessage.ReadInt64()
		return
	}
}
