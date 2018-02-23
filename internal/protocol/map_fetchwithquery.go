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

type mapFetchWithQuery struct {
}

func (self *mapFetchWithQuery) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += DataCalculateSize(args[3].(*Data))
	dataSize += DataCalculateSize(args[4].(*Data))
	return
}
func (self *mapFetchWithQuery) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_FETCHWITHQUERY)
	request.IsRetryable = true
	request.AppendString(args[0].(*string))
	request.AppendInt32(args[1].(int32))
	request.AppendInt32(args[2].(int32))
	request.AppendData(args[3].(*Data))
	request.AppendData(args[4].(*Data))
	request.UpdateFrameLength()
	return
}

func (self *mapFetchWithQuery) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message

	resultsSize := clientMessage.ReadInt32()
	results := make([]interface{}, resultsSize)
	for resultsIndex := 0; resultsIndex < int(resultsSize); resultsIndex++ {
		resultsItem, err := toObject(clientMessage.ReadData())
		if err != nil {
			return nil, err
		}
		results[resultsIndex] = resultsItem
	}
	parameters = results

	parameters = clientMessage.ReadInt32()
	return
}
