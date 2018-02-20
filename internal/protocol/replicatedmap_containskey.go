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
)

type ReplicatedMapContainsKeyResponseParameters struct {
	Response bool
}

func ReplicatedMapContainsKeyCalculateSize(name *string, key *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(key)
	return dataSize
}

func ReplicatedMapContainsKeyEncodeRequest(name *string, key *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ReplicatedMapContainsKeyCalculateSize(name, key))
	clientMessage.SetMessageType(REPLICATEDMAP_CONTAINSKEY)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(key)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ReplicatedMapContainsKeyDecodeResponse(clientMessage *ClientMessage) *ReplicatedMapContainsKeyResponseParameters {
	// Decode response from client message
	parameters := new(ReplicatedMapContainsKeyResponseParameters)
	parameters.Response = clientMessage.ReadBool()
	return parameters
}
