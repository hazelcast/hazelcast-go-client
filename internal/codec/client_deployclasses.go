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
package codec

import (
	. "github.com/hazelcast/go-client"
)

type ClientDeployClassesResponseParameters struct {
}

func ClientDeployClassesCalculateSize(classDefinitions []Pair) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += INT_SIZE_IN_BYTES
	for _, classDefinitionsItem := range classDefinitions {
		key := classDefinitionsItem.Key.(string)
		val := classDefinitionsItem.Value.([]byte)
		dataSize += StringCalculateSize(&key)
		dataSize += INT_SIZE_IN_BYTES
		for range val {
			dataSize += UINT8_SIZE_IN_BYTES
		}
	}
	return dataSize
}

func ClientDeployClassesEncodeRequest(classDefinitions []Pair) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientDeployClassesCalculateSize(classDefinitions))
	clientMessage.SetMessageType(CLIENT_DEPLOYCLASSES)
	clientMessage.IsRetryable = false
	clientMessage.AppendInt(len(classDefinitions))
	for _, classDefinitionsItem := range classDefinitions {
		key := classDefinitionsItem.Key.(string)
		val := classDefinitionsItem.Value.([]byte)
		clientMessage.AppendString(key)
		clientMessage.AppendInt(len(val))
		for _, valItem := range val {
			clientMessage.AppendUint8(valItem)
		}
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// Empty decodeResponse(clientMessage), this message has no parameters to decode
