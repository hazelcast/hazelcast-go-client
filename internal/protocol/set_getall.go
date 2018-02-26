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

type setGetAllCodec struct {
}

func (self *setGetAllCodec) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	return
}
func (self *setGetAllCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(SET_GETALL)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.UpdateFrameLength()
	return
}

func (self *setGetAllCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message

	responseSize := clientMessage.ReadInt32()
	response := make([]interface{}, responseSize)
	for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
		responseItem, err := toObject(clientMessage.ReadData())
		if err != nil {
			return nil, err
		}
		response[responseIndex] = responseItem
	}
	parameters = response

	return
}
