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

type MapFetchWithQueryResponseParameters struct {
	Results                  *[]Data
	NextTableIndexToReadFrom int32
}

func MapFetchWithQueryCalculateSize(name *string, tableIndex int32, batch int32, projection *Data, predicate *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += DataCalculateSize(projection)
	dataSize += DataCalculateSize(predicate)
	return dataSize
}

func MapFetchWithQueryEncodeRequest(name *string, tableIndex int32, batch int32, projection *Data, predicate *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapFetchWithQueryCalculateSize(name, tableIndex, batch, projection, predicate))
	clientMessage.SetMessageType(MAP_FETCHWITHQUERY)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(tableIndex)
	clientMessage.AppendInt32(batch)
	clientMessage.AppendData(projection)
	clientMessage.AppendData(predicate)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapFetchWithQueryDecodeResponse(clientMessage *ClientMessage) *MapFetchWithQueryResponseParameters {
	// Decode response from client message
	parameters := new(MapFetchWithQueryResponseParameters)

	resultsSize := clientMessage.ReadInt32()
	results := make([]Data, resultsSize)
	for resultsIndex := 0; resultsIndex < int(resultsSize); resultsIndex++ {
		resultsItem := clientMessage.ReadData()
		results[resultsIndex] = *resultsItem
	}
	parameters.Results = &results

	parameters.NextTableIndexToReadFrom = clientMessage.ReadInt32()
	return parameters
}
