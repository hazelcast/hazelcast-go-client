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

type mapValuesWithPagingPredicate struct {
}

func (self *mapValuesWithPagingPredicate) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += DataCalculateSize(args[1].(*Data))
	return
}
func (self *mapValuesWithPagingPredicate) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_VALUESWITHPAGINGPREDICATE)
	request.IsRetryable = true
	request.AppendString(args[0].(*string))
	request.AppendData(args[1].(*Data))
	request.UpdateFrameLength()
	return
}

func (self *mapValuesWithPagingPredicate) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message

	responseSize := clientMessage.ReadInt32()
	response := make([]*Pair, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		var responseItem *Pair
		responseItemKey := clientMessage.ReadData()

		responseItemVal := clientMessage.ReadData()

		responseItem.key = responseItemKey
		responseItem.value = responseItemVal
		response[responseIndex] = responseItem
	}
	parameters = response

	return
}
