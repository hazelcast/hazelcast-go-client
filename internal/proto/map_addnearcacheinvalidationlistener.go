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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func mapAddNearCacheInvalidationListenerCalculateSize(name string, listenerFlags int32, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// MapAddNearCacheInvalidationListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapAddNearCacheInvalidationListenerEncodeRequest(name string, listenerFlags int32, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapAddNearCacheInvalidationListenerCalculateSize(name, listenerFlags, localOnly))
	clientMessage.SetMessageType(mapAddNearCacheInvalidationListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(listenerFlags)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapAddNearCacheInvalidationListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapAddNearCacheInvalidationListenerDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	return func() (response string) {
		if clientMessage.IsComplete() {
			return
		}
		response = clientMessage.ReadString()
		return
	}
}

// MapAddNearCacheInvalidationListenerHandleEventIMapInvalidationFunc is the event handler function.
type MapAddNearCacheInvalidationListenerHandleEventIMapInvalidationFunc func(serialization.Data, string, *uuid, int64)

// MapAddNearCacheInvalidationListenerHandleEventIMapBatchInvalidationFunc is the event handler function.
type MapAddNearCacheInvalidationListenerHandleEventIMapBatchInvalidationFunc func([]serialization.Data, []string, []*uuid, []int64)

// MapAddNearCacheInvalidationListenerEventIMapInvalidationDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func MapAddNearCacheInvalidationListenerEventIMapInvalidationDecode(clientMessage *ClientMessage) (
	key serialization.Data, sourceUuid string, partitionUuid *uuid, sequence int64) {
	if clientMessage.IsComplete() {
		return
	}

	if !clientMessage.ReadBool() {
		key = clientMessage.ReadData()
	}
	sourceUuid = clientMessage.ReadString()
	partitionUuid = UUIDCodecDecode(clientMessage)
	sequence = clientMessage.ReadInt64()
	return
}

// MapAddNearCacheInvalidationListenerEventIMapBatchInvalidationDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func MapAddNearCacheInvalidationListenerEventIMapBatchInvalidationDecode(clientMessage *ClientMessage) (
	keys []serialization.Data, sourceUuids []string, partitionUuids []*uuid, sequences []int64) {
	if clientMessage.IsComplete() {
		return
	}
	keysSize := clientMessage.ReadInt32()
	keys = make([]serialization.Data, keysSize)
	for keysIndex := 0; keysIndex < int(keysSize); keysIndex++ {
		keysItem := clientMessage.ReadData()
		keys[keysIndex] = keysItem
	}
	sourceUuidsSize := clientMessage.ReadInt32()
	sourceUuids = make([]string, sourceUuidsSize)
	for sourceUuidsIndex := 0; sourceUuidsIndex < int(sourceUuidsSize); sourceUuidsIndex++ {
		sourceUuidsItem := clientMessage.ReadString()
		sourceUuids[sourceUuidsIndex] = sourceUuidsItem
	}
	partitionUuidsSize := clientMessage.ReadInt32()
	partitionUuids = make([]*uuid, partitionUuidsSize)
	for partitionUuidsIndex := 0; partitionUuidsIndex < int(partitionUuidsSize); partitionUuidsIndex++ {
		partitionUuidsItem := UUIDCodecDecode(clientMessage)
		partitionUuids[partitionUuidsIndex] = partitionUuidsItem
	}
	sequencesSize := clientMessage.ReadInt32()
	sequences = make([]int64, sequencesSize)
	for sequencesIndex := 0; sequencesIndex < int(sequencesSize); sequencesIndex++ {
		sequencesItem := clientMessage.ReadInt64()
		sequences[sequencesIndex] = sequencesItem
	}
	return
}

// MapAddNearCacheInvalidationListenerHandle handles the event with the given
// event handler function.
func MapAddNearCacheInvalidationListenerHandle(clientMessage *ClientMessage,
	handleEventIMapInvalidation MapAddNearCacheInvalidationListenerHandleEventIMapInvalidationFunc,
	handleEventIMapBatchInvalidation MapAddNearCacheInvalidationListenerHandleEventIMapBatchInvalidationFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == bufutil.EventIMapInvalidation && handleEventIMapInvalidation != nil {
		handleEventIMapInvalidation(MapAddNearCacheInvalidationListenerEventIMapInvalidationDecode(clientMessage))
	}
	if messageType == bufutil.EventIMapBatchInvalidation && handleEventIMapBatchInvalidation != nil {
		handleEventIMapBatchInvalidation(MapAddNearCacheInvalidationListenerEventIMapBatchInvalidationDecode(clientMessage))
	}
}
