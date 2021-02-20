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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x030900
	QueueDrainToCodecRequestMessageType = int32(198912)
	// hex: 0x030901
	QueueDrainToCodecResponseMessageType = int32(198913)

	QueueDrainToCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Removes all available elements from this queue and adds them to the given collection.  This operation may be more
// efficient than repeatedly polling this queue.  A failure encountered while attempting to add elements to
// collection c may result in elements being in neither, either or both collections when the associated exception is
// thrown. Attempts to drain a queue to itself result in ILLEGAL_ARGUMENT. Further, the behavior of
// this operation is undefined if the specified collection is modified while the operation is in progress.
type queueDrainToCodec struct{}

var QueueDrainToCodec queueDrainToCodec

func (queueDrainToCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueueDrainToCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueDrainToCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (queueDrainToCodec) DecodeResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return ListMultiFrameCodec.DecodeForData(frameIterator)
}
