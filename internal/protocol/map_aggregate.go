// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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
	. "github.com/hazelcast/go-client/internal/serialization"
)

type MapAggregateResponseParameters struct {
	Response *Data
}

func MapAggregateCalculateSize(name *string, aggregator *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(aggregator)
	return dataSize
}

func MapAggregateEncodeRequest(name *string, aggregator *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapAggregateCalculateSize(name, aggregator))
	clientMessage.SetMessageType(MAP_AGGREGATE)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(aggregator)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapAggregateDecodeResponse(clientMessage *ClientMessage) *MapAggregateResponseParameters {
	// Decode response from client message
	parameters := new(MapAggregateResponseParameters)

	if !clientMessage.ReadBool() {
		parameters.Response = clientMessage.ReadData()
	}
	return parameters
}
