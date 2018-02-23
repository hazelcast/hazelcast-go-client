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

type mapAddNearCacheEntryListener struct {
}

func (self *mapAddNearCacheEntryListener) CalculateSize(args ...interface{}) (dataSize int) {
	// Calculates the request payload size
	dataSize += StringCalculateSize(args[0].(*string))
	dataSize += INT32_SIZE_IN_BYTES
	dataSize += BOOL_SIZE_IN_BYTES
	return
}
func (self *mapAddNearCacheEntryListener) EncodeRequest(args ...interface{}) (request *ClientMessage) {
	// Encode request into clientMessage
	request = NewClientMessage(nil, self.CalculateSize(args))
	request.SetMessageType(MAP_ADDNEARCACHEENTRYLISTENER)
	request.IsRetryable = false
	request.AppendString(args[0].(*string))
	request.AppendInt32(args[1].(int32))
	request.AppendBool(args[2].(bool))
	request.UpdateFrameLength()
	return
}

func (self *mapAddNearCacheEntryListener) DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error) {
	// Decode response from client message
	parameters = clientMessage.ReadString()
	return
}

func (self *mapAddNearCacheEntryListener) Handle(clientMessage *ClientMessage, handleEventIMapInvalidation func(*Data, *string, *Uuid, int64), handleEventIMapBatchInvalidation func(*[]*Data, *[]*string, *[]*Uuid, *[]*int64)) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == EVENT_IMAPINVALIDATION && handleEventIMapInvalidation != nil {
		var key *Data
		if !clientMessage.ReadBool() {
			key = clientMessage.ReadData()

		}
		sourceUuid := clientMessage.ReadString()
		partitionUuid := UuidCodecDecode(clientMessage)
		sequence := clientMessage.ReadInt64()
		handleEventIMapInvalidation(key, sourceUuid, partitionUuid, sequence)
	}

	if messageType == EVENT_IMAPBATCHINVALIDATION && handleEventIMapBatchInvalidation != nil {

		keysSize := clientMessage.ReadInt32()
		keys := make([]interface{}, keysSize)
		for keysIndex := 0; keysIndex < int(keysSize); keysIndex++ {
			keysItem := clientMessage.ReadData()

			keys[keysIndex] = keysItem
		}

		sourceUuidsSize := clientMessage.ReadInt32()
		sourceUuids := make([]string, sourceUuidsSize)
		for sourceUuidsIndex := 0; sourceUuidsIndex < int(sourceUuidsSize); sourceUuidsIndex++ {
			sourceUuidsItem := clientMessage.ReadString()
			sourceUuids[sourceUuidsIndex] = sourceUuidsItem
		}

		partitionUuidsSize := clientMessage.ReadInt32()
		partitionUuids := make([]Uuid, partitionUuidsSize)
		for partitionUuidsIndex := 0; partitionUuidsIndex < int(partitionUuidsSize); partitionUuidsIndex++ {
			partitionUuidsItem := UuidCodecDecode(clientMessage)
			partitionUuids[partitionUuidsIndex] = partitionUuidsItem
		}

		sequencesSize := clientMessage.ReadInt32()
		sequences := make([]int64, sequencesSize)
		for sequencesIndex := 0; sequencesIndex < int(sequencesSize); sequencesIndex++ {
			sequencesItem := clientMessage.ReadInt64()
			sequences[sequencesIndex] = sequencesItem
		}

		handleEventIMapBatchInvalidation(keys, sourceUuids, partitionUuids, sequences)
	}
}
