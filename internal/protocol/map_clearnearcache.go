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
	."github.com/hazelcast/go-client/core"
)

type MapClearNearCacheResponseParameters struct {
}

func MapClearNearCacheCalculateSize(name string, target Address) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += AddressCalculateSize(&target)
	return dataSize
}

func MapClearNearCacheEncodeRequest(name string, target Address) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapClearNearCacheCalculateSize(name, target))
	clientMessage.SetMessageType(MAP_CLEARNEARCACHE)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	AddressCodecEncode(clientMessage, &target)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// Empty decodeResponse(clientMessage), this message has no parameters to decode
