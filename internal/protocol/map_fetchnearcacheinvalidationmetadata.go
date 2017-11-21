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
)

type MapFetchNearCacheInvalidationMetadataResponseParameters struct {
	NamePartitionSequenceList *[]Pair
	PartitionUuidList         *[]Pair
}

func MapFetchNearCacheInvalidationMetadataCalculateSize(names *[]string, address *Address) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += INT_SIZE_IN_BYTES
	for _, namesItem := range *names {
		dataSize += StringCalculateSize(&namesItem)
	}
	dataSize += AddressCalculateSize(address)
	return dataSize
}

func MapFetchNearCacheInvalidationMetadataEncodeRequest(names *[]string, address *Address) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapFetchNearCacheInvalidationMetadataCalculateSize(names, address))
	clientMessage.SetMessageType(MAP_FETCHNEARCACHEINVALIDATIONMETADATA)
	clientMessage.IsRetryable = false
	clientMessage.AppendInt(len(*names))
	for _, namesItem := range *names {
		clientMessage.AppendString(&namesItem)
	}
	AddressCodecEncode(clientMessage, address)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapFetchNearCacheInvalidationMetadataDecodeResponse(clientMessage *ClientMessage) *MapFetchNearCacheInvalidationMetadataResponseParameters {
	// Decode response from client message
	parameters := new(MapFetchNearCacheInvalidationMetadataResponseParameters)

	namePartitionSequenceListSize := clientMessage.ReadInt32()
	namePartitionSequenceList := make([]Pair, namePartitionSequenceListSize)
	for namePartitionSequenceListIndex := 0; namePartitionSequenceListIndex < int(namePartitionSequenceListSize); namePartitionSequenceListIndex++ {
		var namePartitionSequenceListItem Pair
		namePartitionSequenceListItemKey := clientMessage.ReadString()

		namePartitionSequenceListItemValSize := clientMessage.ReadInt32()
		namePartitionSequenceListItemVal := make([]Pair, namePartitionSequenceListItemValSize)
		for namePartitionSequenceListItemValIndex := 0; namePartitionSequenceListItemValIndex < int(namePartitionSequenceListItemValSize); namePartitionSequenceListItemValIndex++ {
			var namePartitionSequenceListItemValItem Pair
			namePartitionSequenceListItemValItemKey := clientMessage.ReadInt32()
			namePartitionSequenceListItemValItemVal := clientMessage.ReadInt64()
			namePartitionSequenceListItemValItem.key = namePartitionSequenceListItemValItemKey
			namePartitionSequenceListItemValItem.value = namePartitionSequenceListItemValItemVal
			namePartitionSequenceListItemVal[namePartitionSequenceListItemValIndex] = namePartitionSequenceListItemValItem
		}

		namePartitionSequenceListItem.key = namePartitionSequenceListItemKey
		namePartitionSequenceListItem.value = namePartitionSequenceListItemVal
		namePartitionSequenceList[namePartitionSequenceListIndex] = namePartitionSequenceListItem
	}
	parameters.NamePartitionSequenceList = &namePartitionSequenceList

	partitionUuidListSize := clientMessage.ReadInt32()
	partitionUuidList := make([]Pair, partitionUuidListSize)
	for partitionUuidListIndex := 0; partitionUuidListIndex < int(partitionUuidListSize); partitionUuidListIndex++ {
		var partitionUuidListItem Pair
		partitionUuidListItemKey := clientMessage.ReadInt32()
		partitionUuidListItemVal := UuidCodecDecode(clientMessage)
		partitionUuidListItem.key = partitionUuidListItemKey
		partitionUuidListItem.value = partitionUuidListItemVal
		partitionUuidList[partitionUuidListIndex] = partitionUuidListItem
	}
	parameters.PartitionUuidList = &partitionUuidList

	return parameters
}
