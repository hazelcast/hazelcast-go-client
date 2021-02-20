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
	// hex: 0x1D0300
	PNCounterGetConfiguredReplicaCountCodecRequestMessageType = int32(1901312)
	// hex: 0x1D0301
	PNCounterGetConfiguredReplicaCountCodecResponseMessageType = int32(1901313)

	PNCounterGetConfiguredReplicaCountCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	PNCounterGetConfiguredReplicaCountResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the configured number of CRDT replicas for the PN counter with
// the given {@code name}.
// The actual replica count may be less, depending on the number of data
// members in the cluster (members that own data).
type pncounterGetConfiguredReplicaCountCodec struct{}

var PNCounterGetConfiguredReplicaCountCodec pncounterGetConfiguredReplicaCountCodec

func (pncounterGetConfiguredReplicaCountCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, PNCounterGetConfiguredReplicaCountCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(PNCounterGetConfiguredReplicaCountCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (pncounterGetConfiguredReplicaCountCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeInt(initialFrame.Content, PNCounterGetConfiguredReplicaCountResponseResponseOffset)
}
