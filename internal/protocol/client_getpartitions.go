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

import ()

func ClientGetPartitionsCalculateSize() int {
	// Calculates the request payload size
	dataSize := 0
	return dataSize
}

func ClientGetPartitionsEncodeRequest() *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientGetPartitionsCalculateSize())
	clientMessage.SetMessageType(CLIENT_GETPARTITIONS)
	clientMessage.IsRetryable = false
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientGetPartitionsDecodeResponse(clientMessage *ClientMessage) func() (partitions []*Pair, partitionStateVersion int32) {
	// Decode response from client message
	return func() (partitions []*Pair, partitionStateVersion int32) {

		partitionsSize := clientMessage.ReadInt32()
		partitions = make([]*Pair, partitionsSize)
		for partitionsIndex := 0; partitionsIndex < int(partitionsSize); partitionsIndex++ {
			partitionsItem_key := AddressCodecDecode(clientMessage)

			partitionsItem_valSize := clientMessage.ReadInt32()
			partitionsItem_val := make([]int32, partitionsItem_valSize)
			for partitionsItem_valIndex := 0; partitionsItem_valIndex < int(partitionsItem_valSize); partitionsItem_valIndex++ {
				partitionsItem_valItem := clientMessage.ReadInt32()
				partitionsItem_val[partitionsItem_valIndex] = partitionsItem_valItem
			}
			var partitionsItem = &Pair{key: partitionsItem_key, value: partitionsItem_val}

			partitions[partitionsIndex] = partitionsItem
		}
		partitionStateVersion = clientMessage.ReadInt32()
		return
	}
}
