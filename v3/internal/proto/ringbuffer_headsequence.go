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

func ringbufferHeadSequenceCalculateSize(name string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	return dataSize
}

// RingbufferHeadSequenceEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func RingbufferHeadSequenceEncodeRequest(name string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ringbufferHeadSequenceCalculateSize(name))
	clientMessage.SetMessageType(ringbufferHeadSequence)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// RingbufferHeadSequenceDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func RingbufferHeadSequenceDecodeResponse(clientMessage *ClientMessage) func() (response int64) {
	// Decode response from client message
	return func() (response int64) {
		response = clientMessage.ReadInt64()
		return
	}
}
