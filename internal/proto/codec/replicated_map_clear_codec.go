/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	// hex: 0x0D0900
	ReplicatedMapClearCodecRequestMessageType = int32(854272)
	// hex: 0x0D0901
	ReplicatedMapClearCodecResponseMessageType = int32(854273)

	ReplicatedMapClearCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// The clear operation wipes data out of the replicated maps.It is the only synchronous remote operation in this
// implementation, so be aware that this might be a slow operation. If some node fails on executing the operation,
// it is retried for at most 3 times (on the failing nodes only). If it does not work after the third time, this
// method throws a OPERATION_TIMEOUT back to the caller.

func EncodeReplicatedMapClearRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ReplicatedMapClearCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapClearCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}
