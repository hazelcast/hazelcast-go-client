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

type listAddListenerCodec struct {
}

func (self *listAddListenerCodec) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return
}
func (self *listAddListenerCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(LIST_ADDLISTENER)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendBool(args[1].(bool))
	request.AppendBool(args[2].(bool))
	request.UpdateFrameLength()
	return
}

func (self *listAddListenerCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message
	parameters = clientMessage.ReadString()
	return
}

func (self *listAddListenerCodec) Handle(clientMessage *ClientMessage, handleEventItem func(*Data, *string, int32)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_ITEM && handleEventItem != nil {
		var item *Data
		if !clientMessage.ReadBool() {
			item = clientMessage.ReadData()

		}
		uuid := clientMessage.ReadString()
		eventType := clientMessage.ReadInt32()
		handleEventItem(item, uuid, eventType)
	}
}
