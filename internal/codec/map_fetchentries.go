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
	. "github.com/hazelcast/go-client/internal"
)

type MapFetchEntriesResponseParameters struct {
	TableIndex int32
	Entries    []Pair
}

func MapFetchEntriesCalculateSize(name string, partitionId int32, tableIndex int32, batch int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	return dataSize
}

func MapFetchEntriesEncodeRequest(name string, partitionId int32, tableIndex int32, batch int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapFetchEntriesCalculateSize(name, partitionId, tableIndex, batch))
	clientMessage.SetMessageType(MAP_FETCHENTRIES)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(partitionId)
	clientMessage.AppendInt32(tableIndex)
	clientMessage.AppendInt32(batch)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapFetchEntriesDecodeResponse(clientMessage *ClientMessage) *MapFetchEntriesResponseParameters {
	// Decode response from client message
	parameters := new(MapFetchEntriesResponseParameters)
	parameters.TableIndex = clientMessage.ReadInt32()

	entriesSize := clientMessage.ReadInt32()
	entries := make([]Pair, entriesSize)
	for entriesIndex := 0; entriesIndex < int(entriesSize); entriesIndex++ {
		var entriesItem Pair
		entriesItemKey := clientMessage.ReadData()
		entriesItemVal := clientMessage.ReadData()
		entriesItem.Key = entriesItemKey
		entriesItem.Value = entriesItemVal
		entries = append(entries, entriesItem)
	}
	parameters.Entries = entries

	return parameters
}
