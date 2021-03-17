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

func multimapTryLockCalculateSize(name string, key serialization.Data, threadId int64, lease int64, timeout int64, referenceId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(key)
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.Int64SizeInBytes
	return dataSize
}

// MultiMapTryLockEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MultiMapTryLockEncodeRequest(name string, key serialization.Data, threadId int64, lease int64, timeout int64, referenceId int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, multimapTryLockCalculateSize(name, key, threadId, lease, timeout, referenceId))
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

// MultiMapTryLockDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MultiMapTryLockDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
