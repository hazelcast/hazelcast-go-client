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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func FlakeIdGeneratorNewIdBatchCalculateSize(name *string, batchSize int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += common.Int32SizeInBytes
	return dataSize
}

func FlakeIdGeneratorNewIdBatchEncodeRequest(name *string, batchSize int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, FlakeIdGeneratorNewIdBatchCalculateSize(name, batchSize))
	clientMessage.SetMessageType(flakeidgeneratorNewIdBatch)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(batchSize)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func FlakeIdGeneratorNewIdBatchDecodeResponse(clientMessage *ClientMessage) func() (base int64, increment int64, batchSize int32) {
	// Decode response from client message
	return func() (base int64, increment int64, batchSize int32) {
		if clientMessage.IsComplete() {
			return
		}
		base = clientMessage.ReadInt64()
		increment = clientMessage.ReadInt64()
		batchSize = clientMessage.ReadInt32()
		return
	}
}
