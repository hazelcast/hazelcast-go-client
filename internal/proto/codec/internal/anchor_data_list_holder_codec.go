/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type anchordatalistholderCodec struct{}

var AnchorDataListHolderCodec anchordatalistholderCodec

func (anchordatalistholderCodec) Encode(clientMessage *proto.ClientMessage, anchorDataListHolder proto.AnchorDataListHolder) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())

	ListIntegerCodec.Encode(clientMessage, anchorDataListHolder.GetAnchorPageList())
	EntryListCodec.EncodeForDataAndData(clientMessage, anchorDataListHolder.GetAnchorDataList())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (anchordatalistholderCodec) Decode(frameIterator *proto.ForwardFrameIterator) proto.AnchorDataListHolder {
	// begin frame
	frameIterator.Next()

	anchorPageList := ListIntegerCodec.Decode(frameIterator)
	anchorDataList := EntryListCodec.Decode(frameIterator, DataCodec.Decode, DataCodec.DecodeNullable)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return proto.NewAnchorDataListHolder(anchorPageList, anchorDataList)
}
