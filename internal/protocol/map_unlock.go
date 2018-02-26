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
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"

	. "github.com/hazelcast/hazelcast-go-client/internal/common"
)

type mapUnlockCodec struct {
}

func (self *mapUnlockCodec) CalculateSize(args ...interface{}) (dataSize int) {
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += DataCalculateSize(args[1].(*Data))
	dataSize += INT64_SIZE_IN_BYTES
	dataSize += INT64_SIZE_IN_BYTES
	return
}
func (self *mapUnlockCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(MAP_UNLOCK)
	request.IsRetryable = true
	request.AppendString(args[0].(*string))
	request.AppendData(args[1].(*Data))
	request.AppendInt64(args[2].(int64))
	request.AppendInt64(args[3].(int64))
	request.UpdateFrameLength()
	return
}

// Empty DecodeResponse(), this message has no parameters to decode
func (*mapUnlockCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	return nil, nil
}
