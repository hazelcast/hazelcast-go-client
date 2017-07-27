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

type MapEventJournalReadResponseParameters struct {
	ReadCount int32
	Items     []Data
	ItemSeqs  []int64
}

func MapEventJournalReadCalculateSize(name string, startSequence int64, minSize int32, maxSize int32, predicate *Data, projection *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	if predicate != nil {
		dataSize += DataCalculateSize(predicate)
	}
	dataSize += BOOL_SIZE_IN_BYTES
	if projection != nil {
		dataSize += DataCalculateSize(projection)
	}
	return dataSize
}

func MapEventJournalReadEncodeRequest(name string, startSequence int64, minSize int32, maxSize int32, predicate *Data, projection *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapEventJournalReadCalculateSize(name, startSequence, minSize, maxSize, predicate, projection))
	clientMessage.SetMessageType(MAP_EVENTJOURNALREAD)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt64(startSequence)
	clientMessage.AppendInt32(minSize)
	clientMessage.AppendInt32(maxSize)
	clientMessage.AppendBool(predicate == nil)
	if predicate != nil {
		clientMessage.AppendData(*predicate)
	}
	clientMessage.AppendBool(projection == nil)
	if projection != nil {
		clientMessage.AppendData(*projection)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapEventJournalReadDecodeResponse(clientMessage *ClientMessage) *MapEventJournalReadResponseParameters {
	// Decode response from client message
	parameters := new(MapEventJournalReadResponseParameters)
	parameters.ReadCount = clientMessage.ReadInt32()

	itemsSize := clientMessage.ReadInt32()
	items := make([]Data, itemsSize)
	for itemsIndex := 0; itemsIndex < int(itemsSize); itemsIndex++ {
		itemsItem := clientMessage.ReadData()
		items = append(items, itemsItem)
	}
	parameters.Items = items

	if !clientMessage.ReadBool() {

		itemSeqsSize := clientMessage.ReadInt32()
		itemSeqs := make([]int64, itemSeqsSize)
		for itemSeqsIndex := 0; itemSeqsIndex < int(itemSeqsSize); itemSeqsIndex++ {
			itemSeqsItem := clientMessage.ReadInt64()
			itemSeqs = append(itemSeqs, itemSeqsItem)
		}
		parameters.ItemSeqs = itemSeqs

	}
	return parameters
}
