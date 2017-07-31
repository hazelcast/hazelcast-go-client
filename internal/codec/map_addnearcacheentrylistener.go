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

type MapAddNearCacheEntryListenerResponseParameters struct {
	Response string
}

func MapAddNearCacheEntryListenerCalculateSize(name string, listenerFlags int32, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(&name)
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return dataSize
}

func MapAddNearCacheEntryListenerEncodeRequest(name string, listenerFlags int32, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapAddNearCacheEntryListenerCalculateSize(name, listenerFlags, localOnly))
	clientMessage.SetMessageType(MAP_ADDNEARCACHEENTRYLISTENER)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(listenerFlags)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapAddNearCacheEntryListenerDecodeResponse(clientMessage *ClientMessage) *MapAddNearCacheEntryListenerResponseParameters {
	// Decode response from client message
	parameters := new(MapAddNearCacheEntryListenerResponseParameters)
	parameters.Response = *clientMessage.ReadString()
	return parameters
}

func MapAddNearCacheEntryListenerHandle(clientMessage *ClientMessage, handleEventIMapInvalidation func(Data, string, Uuid, int64), handleEventIMapBatchInvalidation func([]Data, []string, []Uuid, []int64)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_IMAPINVALIDATION && handleEventIMapInvalidation != nil {
		var key Data
		if !clientMessage.ReadBool() {
			key = clientMessage.ReadData()
		}
		sourceUuid := *clientMessage.ReadString()
		partitionUuid := *UuidCodecDecode(clientMessage)
		sequence := clientMessage.ReadInt64()
		handleEventIMapInvalidation(key, sourceUuid, partitionUuid, sequence)
	}

	if messageType == EVENT_IMAPBATCHINVALIDATION && handleEventIMapBatchInvalidation != nil {

		keysSize := clientMessage.ReadInt32()
		keys := make([]Data, keysSize)
		for keysIndex := 0; keysIndex < int(keysSize); keysIndex++ {
			keysItem := clientMessage.ReadData()
			keys = append(keys, keysItem)
		}

		sourceUuidsSize := clientMessage.ReadInt32()
		sourceUuids := make([]string, sourceUuidsSize)
		for sourceUuidsIndex := 0; sourceUuidsIndex < int(sourceUuidsSize); sourceUuidsIndex++ {
			sourceUuidsItem := *clientMessage.ReadString()
			sourceUuids = append(sourceUuids, sourceUuidsItem)
		}

		partitionUuidsSize := clientMessage.ReadInt32()
		partitionUuids := make([]Uuid, partitionUuidsSize)
		for partitionUuidsIndex := 0; partitionUuidsIndex < int(partitionUuidsSize); partitionUuidsIndex++ {
			partitionUuidsItem := *UuidCodecDecode(clientMessage)
			partitionUuids = append(partitionUuids, partitionUuidsItem)
		}

		sequencesSize := clientMessage.ReadInt32()
		sequences := make([]int64, sequencesSize)
		for sequencesIndex := 0; sequencesIndex < int(sequencesSize); sequencesIndex++ {
			sequencesItem := clientMessage.ReadInt64()
			sequences = append(sequences, sequencesItem)
		}

		handleEventIMapBatchInvalidation(keys, sourceUuids, partitionUuids, sequences)
	}
}
