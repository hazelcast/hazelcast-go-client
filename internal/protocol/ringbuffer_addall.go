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

func RingbufferAddAllCalculateSize(name *string, valueList []*serialization.Data, overflowPolicy int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	for _, valueListItem := range valueList {
		dataSize += dataCalculateSize(valueListItem)
	}
	dataSize += bufutil.Int32SizeInBytes
	return dataSize
}

func RingbufferAddAllEncodeRequest(name *string, valueList []*serialization.Data, overflowPolicy int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, RingbufferAddAllCalculateSize(name, valueList, overflowPolicy))
	clientMessage.SetMessageType(ringbufferAddAll)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(int32(len(valueList)))
	for _, valueListItem := range valueList {
		clientMessage.AppendData(valueListItem)
	}
	clientMessage.AppendInt32(overflowPolicy)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func RingbufferAddAllDecodeResponse(clientMessage *ClientMessage) func() (response int64) {
	// Decode response from client message
	return func() (response int64) {
		response = clientMessage.ReadInt64()
		return
	}
}
