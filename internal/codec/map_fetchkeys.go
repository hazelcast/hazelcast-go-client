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

type MapFetchKeysResponseParameters struct {
	TableIndex int32
	Keys       []Data
}

func MapFetchKeysCalculateSize(name string, partitionId int32, tableIndex int32, batch int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	return dataSize
}

func MapFetchKeysEncodeRequest(name string, partitionId int32, tableIndex int32, batch int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapFetchKeysCalculateSize(name, partitionId, tableIndex, batch))
	clientMessage.SetMessageType(MAP_FETCHKEYS)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(partitionId)
	clientMessage.AppendInt32(tableIndex)
	clientMessage.AppendInt32(batch)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapFetchKeysDecodeResponse(clientMessage *ClientMessage) *MapFetchKeysResponseParameters {
	// Decode response from client message
	parameters := new(MapFetchKeysResponseParameters)
	parameters.TableIndex = clientMessage.ReadInt32()

	keysSize := clientMessage.ReadInt32()
	keys := make([]Data, keysSize)
	for keysIndex := 0; keysIndex < int(keysSize); keysIndex++ {
		keysItem := clientMessage.ReadData()
		keys = append(keys, keysItem)
	}
	parameters.Keys = keys

	return parameters
}
