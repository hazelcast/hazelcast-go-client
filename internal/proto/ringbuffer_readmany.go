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

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func ringbufferReadManyCalculateSize(name string, startSequence int64, minCount int32, maxCount int32, filter serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	if filter != nil {
		dataSize += dataCalculateSize(filter)
	}
	return dataSize
}

// RingbufferReadManyEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func RingbufferReadManyEncodeRequest(name string, startSequence int64, minCount int32, maxCount int32, filter serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// RingbufferReadManyDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func RingbufferReadManyDecodeResponse(clientMessage *ClientMessage) func() (readCount int32, items []serialization.Data, itemSeqs []int64, nextSeq int64) {
	// Decode response from client message
	//TODO
	return nil
}
