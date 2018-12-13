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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func mapFetchNearCacheInvalidationMetadataCalculateSize(names []string, address *Address) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += bufutil.Int32SizeInBytes
	for _, namesItem := range names {
		dataSize += stringCalculateSize(namesItem)
	}
	dataSize += addressCalculateSize(address)
	return dataSize
}

// MapFetchNearCacheInvalidationMetadataEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapFetchNearCacheInvalidationMetadataEncodeRequest(names []string, address *Address) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapFetchNearCacheInvalidationMetadataCalculateSize(names, address))
	clientMessage.SetMessageType(mapFetchNearCacheInvalidationMetadata)
	clientMessage.IsRetryable = false
	clientMessage.AppendInt32(int32(len(names)))
	for _, namesItem := range names {
		clientMessage.AppendString(namesItem)
	}
	AddressCodecEncode(clientMessage, address)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapFetchNearCacheInvalidationMetadataDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapFetchNearCacheInvalidationMetadataDecodeResponse(clientMessage *ClientMessage) func() (namePartitionSequenceList []*Pair, partitionUuidList []*Pair) {
	// Decode response from client message
	return func() (namePartitionSequenceList []*Pair, partitionUuidList []*Pair) {
		if clientMessage.IsComplete() {
			return
		}
		namePartitionSequenceListSize := clientMessage.ReadInt32()
		namePartitionSequenceList = make([]*Pair, namePartitionSequenceListSize)
		for namePartitionSequenceListIndex := 0; namePartitionSequenceListIndex < int(namePartitionSequenceListSize); namePartitionSequenceListIndex++ {
			namePartitionSequenceListItemKey := clientMessage.ReadString()
			namePartitionSequenceListItemValueSize := clientMessage.ReadInt32()
			namePartitionSequenceListItemValue := make([]*Pair, namePartitionSequenceListItemValueSize)
			for namePartitionSequenceListItemValueIndex := 0; namePartitionSequenceListItemValueIndex < int(namePartitionSequenceListItemValueSize); namePartitionSequenceListItemValueIndex++ {
				namePartitionSequenceListItemValueItemKey := clientMessage.ReadInt32()
				namePartitionSequenceListItemValueItemValue := clientMessage.ReadInt64()
				var namePartitionSequenceListItemValueItem = &Pair{key: namePartitionSequenceListItemValueItemKey, value: namePartitionSequenceListItemValueItemValue}
				namePartitionSequenceListItemValue[namePartitionSequenceListItemValueIndex] = namePartitionSequenceListItemValueItem
			}
			var namePartitionSequenceListItem = &Pair{key: namePartitionSequenceListItemKey, value: namePartitionSequenceListItemValue}
			namePartitionSequenceList[namePartitionSequenceListIndex] = namePartitionSequenceListItem
		}
		partitionUuidListSize := clientMessage.ReadInt32()
		partitionUuidList = make([]*Pair, partitionUuidListSize)
		for partitionUuidListIndex := 0; partitionUuidListIndex < int(partitionUuidListSize); partitionUuidListIndex++ {
			partitionUuidListItemKey := clientMessage.ReadInt32()
			partitionUuidListItemValue := UUIDCodecDecode(clientMessage)
			var partitionUuidListItem = &Pair{key: partitionUuidListItemKey, value: partitionUuidListItemValue}
			partitionUuidList[partitionUuidListIndex] = partitionUuidListItem
		}
		return
	}
}
