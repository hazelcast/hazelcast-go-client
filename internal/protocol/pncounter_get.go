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
)

func PNCounterGetCalculateSize(name *string, replicaTimestamps []*Pair, targetReplica *Address) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += INT_SIZE_IN_BYTES
	for _, replicaTimestampsItem := range replicaTimestamps {
		key := replicaTimestampsItem.key.(*string)
		val := replicaTimestampsItem.value.(int64)
		dataSize += StringCalculateSize(key)
		dataSize += Int64CalculateSize(val)
	}
	dataSize += AddressCalculateSize(targetReplica)
	return dataSize
}

func PNCounterGetEncodeRequest(name *string, replicaTimestamps []*Pair, targetReplica *Address) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, PNCounterGetCalculateSize(name, replicaTimestamps, targetReplica))
	clientMessage.SetMessageType(PNCOUNTER_GET)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(int32(len(replicaTimestamps)))
	for _, replicaTimestampsItem := range replicaTimestamps {
		key := replicaTimestampsItem.key.(*string)
		val := replicaTimestampsItem.value.(int64)
		clientMessage.AppendString(key)
		clientMessage.AppendInt64(val)
	}
	AddressCodecEncode(clientMessage, targetReplica)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func PNCounterGetDecodeResponse(clientMessage *ClientMessage) func() (value int64, replicaTimestamps []*Pair, replicaCount int32) {
	// Decode response from client message
	return func() (value int64, replicaTimestamps []*Pair, replicaCount int32) {
		if clientMessage.IsComplete() {
			return
		}
		value = clientMessage.ReadInt64()
		replicaTimestampsSize := clientMessage.ReadInt32()
		replicaTimestamps = make([]*Pair, replicaTimestampsSize)
		for replicaTimestampsIndex := 0; replicaTimestampsIndex < int(replicaTimestampsSize); replicaTimestampsIndex++ {
			replicaTimestampsItemKey := clientMessage.ReadString()
			replicaTimestampsItemValue := clientMessage.ReadInt64()
			var replicaTimestampsItem = &Pair{key: replicaTimestampsItemKey, value: replicaTimestampsItemValue}
			replicaTimestamps[replicaTimestampsIndex] = replicaTimestampsItem
		}
		replicaCount = clientMessage.ReadInt32()
		return
	}
}
