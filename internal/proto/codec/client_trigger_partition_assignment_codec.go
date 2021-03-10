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
)

const (
	// hex: 0x001000
	ClientTriggerPartitionAssignmentCodecRequestMessageType = int32(4096)
	// hex: 0x001001
	ClientTriggerPartitionAssignmentCodecResponseMessageType = int32(4097)

	ClientTriggerPartitionAssignmentCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Triggers partition assignment manually on the cluster.
// Note that Partition based operations triggers this automatically

func EncodeClientTriggerPartitionAssignmentRequest() *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientTriggerPartitionAssignmentCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientTriggerPartitionAssignmentCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}
