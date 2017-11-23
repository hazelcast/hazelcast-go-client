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

type MapAggregateWithPredicateResponseParameters struct {
	Response *Data
}

func MapAggregateWithPredicateCalculateSize(name *string, aggregator *Data, predicate *Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += DataCalculateSize(aggregator)
	dataSize += DataCalculateSize(predicate)
	return dataSize
}

func MapAggregateWithPredicateEncodeRequest(name *string, aggregator *Data, predicate *Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapAggregateWithPredicateCalculateSize(name, aggregator, predicate))
	clientMessage.SetMessageType(MAP_AGGREGATEWITHPREDICATE)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(aggregator)
	clientMessage.AppendData(predicate)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapAggregateWithPredicateDecodeResponse(clientMessage *ClientMessage) *MapAggregateWithPredicateResponseParameters {
	// Decode response from client message
	parameters := new(MapAggregateWithPredicateResponseParameters)

	if !clientMessage.ReadBool() {
		parameters.Response = clientMessage.ReadData()
	}
	return parameters
}
