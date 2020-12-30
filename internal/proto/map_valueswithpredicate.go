// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func mapValuesWithPredicateCalculateSize(name string, predicate serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(predicate)
	return dataSize
}

// MapValuesWithPredicateEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapValuesWithPredicateEncodeRequest(name string, predicate serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// MapValuesWithPredicateDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapValuesWithPredicateDecodeResponse(clientMessage *ClientMessage) func() (response []serialization.Data) {
	// Decode response from client message
	//TODO
	return nil
}
