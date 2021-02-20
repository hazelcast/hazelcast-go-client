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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

func mapExecuteOnKeysCalculateSize(name string, entryProcessor serialization.Data, keys []serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(entryProcessor)
	dataSize += bufutil.Int32SizeInBytes
	for _, keysItem := range keys {
		dataSize += dataCalculateSize(keysItem)
	}
	return dataSize
}

// MapExecuteOnKeysEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapExecuteOnKeysEncodeRequest(name string, entryProcessor serialization.Data, keys []serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// MapExecuteOnKeysDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapExecuteOnKeysDecodeResponse(clientMessage *ClientMessage) func() (response []*Pair) {
	// Decode response from client message
	//TODO
	return nil
}
