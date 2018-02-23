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

package protocol

type mapFetchNearCacheInvalidationMetadata struct {
}

func (self *mapFetchNearCacheInvalidationMetadata) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += INT_SIZE_IN_BYTES
	for _, namesItem := range args[0].([]*string) {
		dataSize += StringCalculateSize(namesItem)
	}
	dataSize += AddressCalculateSize(args[1].(*Address))
	return
}
func (self *mapFetchNearCacheInvalidationMetadata) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_FETCHNEARCACHEINVALIDATIONMETADATA)
	request.IsRetryable = false
	request.AppendInt(len(args[0].([]*string)))
	for _, namesItem := range args[0].([]*string) {
		request.AppendString(namesItem)
	}
	AddressCodecEncode(request, args[1].(*Address))
	request.UpdateFrameLength()
	return
}

func (self *mapFetchNearCacheInvalidationMetadata) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message

	namePartitionSequenceListSize := clientMessage.ReadInt32()
	namePartitionSequenceList := make([]*Pair, namePartitionSequenceListSize)
	for namePartitionSequenceListIndex := 0; namePartitionSequenceListIndex < int(namePartitionSequenceListSize); namePartitionSequenceListIndex++ {
		var namePartitionSequenceListItem *Pair
		namePartitionSequenceListItemKey := clientMessage.ReadString()

		namePartitionSequenceListItemValSize := clientMessage.ReadInt32()
		namePartitionSequenceListItemVal := make([]*Pair, namePartitionSequenceListItemValSize)
		for namePartitionSequenceListItemValIndex := 0; namePartitionSequenceListItemValIndex < int(namePartitionSequenceListItemValSize); namePartitionSequenceListItemValIndex++ {
			var namePartitionSequenceListItemValItem *Pair
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
	parameters = namePartitionSequenceList

	partitionUuidListSize := clientMessage.ReadInt32()
	partitionUuidList := make([]*Pair, partitionUuidListSize)
	for partitionUuidListIndex := 0; partitionUuidListIndex < int(partitionUuidListSize); partitionUuidListIndex++ {
		var partitionUuidListItem *Pair
		partitionUuidListItemKey := clientMessage.ReadInt32()
		partitionUuidListItemVal := UuidCodecDecode(clientMessage)
		partitionUuidListItem.key = partitionUuidListItemKey
		partitionUuidListItem.value = partitionUuidListItemVal
		partitionUuidList[partitionUuidListIndex] = partitionUuidListItem
	}
	parameters = partitionUuidList

	return
}
