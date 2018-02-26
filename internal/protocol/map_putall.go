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

import (
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type mapPutAllCodec struct {
}

func (self *mapPutAllCodec) CalculateSize(args ...interface{}) (dataSize int) {
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += INT_SIZE_IN_BYTES
	for _, entriesItem := range args[1].([]*Pair) {
		key := entriesItem.key.(*Data)
		val := entriesItem.value.(*Data)
		dataSize += DataCalculateSize(key)
		dataSize += DataCalculateSize(val)
	}
	return
}
func (self *mapPutAllCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(MAP_PUTALL)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendInt(len(args[1].([]*Pair)))
	for _, entriesItem := range args[1].([]*Pair) {
		key := entriesItem.key.(*Data)
		val := entriesItem.value.(*Data)
		request.AppendData(key)
		request.AppendData(val)
	}
	request.UpdateFrameLength()
	return
}

// Empty DecodeResponse(), this message has no parameters to decode
func (*mapPutAllCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	return nil, nil
}
