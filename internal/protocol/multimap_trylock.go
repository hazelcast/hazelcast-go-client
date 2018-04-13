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

func MultiMapTryLockCalculateSize(name *string, key *Data, threadId int64, lease int64, timeout int64, referenceId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(key)
	dataSize += Int64SizeInBytes
	dataSize += Int64SizeInBytes
	dataSize += Int64SizeInBytes
	dataSize += Int64SizeInBytes
	return dataSize
}

func MultiMapTryLockEncodeRequest(name *string, key *Data, threadId int64, lease int64, timeout int64, referenceId int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MultiMapTryLockCalculateSize(name, key, threadId, lease, timeout, referenceId))
	clientMessage.SetMessageType(multimapTryLock)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendInt64(threadId)
	clientMessage.AppendInt64(lease)
	clientMessage.AppendInt64(timeout)
	clientMessage.AppendInt64(referenceId)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MultiMapTryLockDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
