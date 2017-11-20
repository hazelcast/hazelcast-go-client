// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

type MapLoadGivenKeysResponseParameters struct {
}

func MapLoadGivenKeysCalculateSize(name *string, keys *[]Data, replaceExistingValues bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT_SIZE_IN_BYTES
	for _, keysItem := range *keys {
		dataSize += DataCalculateSize(&keysItem)
	}
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func MapLoadGivenKeysEncodeRequest(name *string, keys *[]Data, replaceExistingValues bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapLoadGivenKeysCalculateSize(name, keys, replaceExistingValues))
	clientMessage.SetMessageType(MAP_LOADGIVENKEYS)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt(len(*keys))
	for _, keysItem := range *keys {
		clientMessage.AppendData(&keysItem)
	}
	clientMessage.AppendBool(replaceExistingValues)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// Empty decodeResponse(clientMessage), this message has no parameters to decode
