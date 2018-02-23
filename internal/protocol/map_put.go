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

type mapPut struct {
}

func (self *mapPut) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += DataCalculateSize(args[1].(*Data))
	dataSize += DataCalculateSize(args[2].(*Data))
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT64_SIZE_IN_BYTES
	return
}
func (self *mapPut) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_PUT)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendData(args[1].(*Data))
	request.AppendData(args[2].(*Data))
	request.AppendInt64(args[3].(int64))
	request.AppendInt64(args[4].(int64))
	request.UpdateFrameLength()
	return
}

func (self *mapPut) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message

	if !clientMessage.ReadBool() {
		parameters, err = toObject(clientMessage.ReadData())
		if err != nil {
			return nil, err
		}
	}
	return
}
