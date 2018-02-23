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

type mapAddIndex struct {
}

func (self *mapAddIndex) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += StringCalculateSize(args[1].(*string))
	dataSize += BOOL_SIZE_IN_BYTES
	return
}
func (self *mapAddIndex) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_ADDINDEX)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendString(args[1].(*string))
	request.AppendBool(args[2].(bool))
	request.UpdateFrameLength()
	return
}

// Empty DecodeResponse(), this message has no parameters to decode
