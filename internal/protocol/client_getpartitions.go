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

type clientGetPartitionsCodec struct {
}

func (self *clientGetPartitionsCodec) CalculateSize(args ...interface{}) (dataSize int) {
	return
}
func (self *clientGetPartitionsCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(CLIENT_GETPARTITIONS)
	request.IsRetryable = false
	request.UpdateFrameLength()
	return
}

func (self *clientGetPartitionsCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {

	partitionsSize := clientMessage.ReadInt32()
	partitions := make([]*Pair, partitionsSize)
	for partitionsIndex := 0; partitionsIndex < int(partitionsSize); partitionsIndex++ {
		var partitionsItem = &Pair{}
		partitionsItemKey := AddressCodecDecode(clientMessage)

		partitionsItemValSize := clientMessage.ReadInt32()
		partitionsItemVal := make([]int32, partitionsItemValSize)
		for partitionsItemValIndex := 0; partitionsItemValIndex < int(partitionsItemValSize); partitionsItemValIndex++ {
			partitionsItemValItem := clientMessage.ReadInt32()
			partitionsItemVal[partitionsItemValIndex] = partitionsItemValItem
		}

		partitionsItem.key = partitionsItemKey
		partitionsItem.value = partitionsItemVal
		partitions[partitionsIndex] = partitionsItem
	}
	parameters = partitions

	return
}
