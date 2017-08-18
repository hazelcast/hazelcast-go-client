// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization"
)

type MapPutResponseParameters struct {
	Response *Data
}

func MapPutCalculateSize(name *string, key *Data, value *Data, threadId int64, ttl int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(key)
	dataSize += DataCalculateSize(value)
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT64_SIZE_IN_BYTES
	return dataSize
}

func MapPutEncodeRequest(name *string, key *Data, value *Data, threadId int64, ttl int64) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapPutCalculateSize(name, key, value, threadId, ttl))
	clientMessage.SetMessageType(MAP_PUT)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.AppendData(value)
	clientMessage.AppendInt64(threadId)
	clientMessage.AppendInt64(ttl)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapPutDecodeResponse(clientMessage *ClientMessage) *MapPutResponseParameters {
	// Decode response from client message
	parameters := new(MapPutResponseParameters)

	if !clientMessage.ReadBool() {
		parameters.Response = clientMessage.ReadData()
	}
	return parameters
}
