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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x000300
	ClientAddClusterViewListenerCodecRequestMessageType = int32(768)
	// hex: 0x000301
	ClientAddClusterViewListenerCodecResponseMessageType = int32(769)

	// hex: 0x000302
	ClientAddClusterViewListenerCodecEventMembersViewMessageType = int32(770)

	// hex: 0x000303
	ClientAddClusterViewListenerCodecEventPartitionsViewMessageType = int32(771)

	ClientAddClusterViewListenerCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ClientAddClusterViewListenerEventMembersViewVersionOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientAddClusterViewListenerEventPartitionsViewVersionOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Adds a cluster view listener to a connection.
type clientAddClusterViewListenerCodec struct{}

var ClientAddClusterViewListenerCodec clientAddClusterViewListenerCodec

func (clientAddClusterViewListenerCodec) EncodeRequest() *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ClientAddClusterViewListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientAddClusterViewListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func (clientAddClusterViewListenerCodec) Handle(clientMessage *proto.ClientMessage, handleMembersViewEvent func(version int32, memberInfos []core.MemberInfo), handlePartitionsViewEvent func(version int32, partitions []proto.Pair)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ClientAddClusterViewListenerCodecEventMembersViewMessageType {
		initialFrame := frameIterator.Next()
		version := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAddClusterViewListenerEventMembersViewVersionOffset)
		memberInfos := internal.ListMultiFrameCodec.DecodeForMemberInfo(frameIterator)
		handleMembersViewEvent(version, memberInfos)
		return
	}
	if messageType == ClientAddClusterViewListenerCodecEventPartitionsViewMessageType {
		initialFrame := frameIterator.Next()
		version := internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, ClientAddClusterViewListenerEventPartitionsViewVersionOffset)
		partitions := internal.EntryListUUIDListIntegerCodec.Decode(frameIterator)
		handlePartitionsViewEvent(version, partitions)
		return
	}
}
