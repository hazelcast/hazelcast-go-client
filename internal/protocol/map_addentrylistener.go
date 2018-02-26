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

type mapAddEntryListenerCodec struct {
}

func (self *mapAddEntryListenerCodec) CalculateSize(args ...interface{}) (dataSize int) {
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += BOOL_SIZE_IN_BYTES
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return
}
func (self *mapAddEntryListenerCodec) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args...))
	request.SetMessageType(MAP_ADDENTRYLISTENER)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendBool(args[1].(bool))
	request.AppendInt32(args[2].(int32))
	request.AppendBool(args[3].(bool))
	request.UpdateFrameLength()
	return
}

func (self *mapAddEntryListenerCodec) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	parameters = clientMessage.ReadString()
	return
}

func (self *mapAddEntryListenerCodec) Handle(clientMessage *ClientMessage, handleEventEntry func(*Data, *Data, *Data, *Data, int32, *string, int32)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_ENTRY && handleEventEntry != nil {
		var key *Data
		if !clientMessage.ReadBool() {
			key = clientMessage.ReadData()

		}
		var value *Data
		if !clientMessage.ReadBool() {
			value = clientMessage.ReadData()

		}
		var oldValue *Data
		if !clientMessage.ReadBool() {
			oldValue = clientMessage.ReadData()

		}
		var mergingValue *Data
		if !clientMessage.ReadBool() {
			mergingValue = clientMessage.ReadData()

		}
		eventType := clientMessage.ReadInt32()
		uuid := clientMessage.ReadString()
		numberOfAffectedEntries := clientMessage.ReadInt32()
		handleEventEntry(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
	}
}
