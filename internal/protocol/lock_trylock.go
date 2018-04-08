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
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
)

func LockTryLockCalculateSize(name *string, timeout int64, leaseTime int64, threadId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT64_SIZE_IN_BYTES
	return dataSize
}

func LockTryLockEncodeRequest(name *string, timeout int64, leaseTime int64, threadId int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, LockTryLockCalculateSize(name, timeout, leaseTime, threadId))
	clientMessage.SetMessageType(LOCK_TRYLOCK)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt64(threadId)
	clientMessage.AppendInt64(leaseTime)
	clientMessage.AppendInt64(timeout)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func LockTryLockDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
