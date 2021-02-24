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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	PagingPredicateHolderCodecPageSizeFieldOffset             = 0
	PagingPredicateHolderCodecPageFieldOffset                 = PagingPredicateHolderCodecPageSizeFieldOffset + proto.IntSizeInBytes
	PagingPredicateHolderCodecIterationTypeIdFieldOffset      = PagingPredicateHolderCodecPageFieldOffset + proto.IntSizeInBytes
	PagingPredicateHolderCodecIterationTypeIdInitialFrameSize = PagingPredicateHolderCodecIterationTypeIdFieldOffset + proto.ByteSizeInBytes
)

/*
type pagingpredicateholderCodec struct {}

var PagingPredicateHolderCodec pagingpredicateholderCodec
*/

func EncodePagingPredicateHolder(clientMessage *proto.ClientMessage, pagingPredicateHolder proto.PagingPredicateHolder) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, PagingPredicateHolderCodecIterationTypeIdInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, PagingPredicateHolderCodecPageSizeFieldOffset, int32(pagingPredicateHolder.PageSize()))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, PagingPredicateHolderCodecPageFieldOffset, int32(pagingPredicateHolder.Page()))
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, PagingPredicateHolderCodecIterationTypeIdFieldOffset, pagingPredicateHolder.IterationTypeId())
	clientMessage.AddFrame(initialFrame)

	EncodeAnchorDataListHolder(clientMessage, pagingPredicateHolder.AnchorDataListHolder())
	CodecUtil.EncodeNullableForData(clientMessage, pagingPredicateHolder.PredicateData())
	CodecUtil.EncodeNullableForData(clientMessage, pagingPredicateHolder.ComparatorData())
	CodecUtil.EncodeNullableForData(clientMessage, pagingPredicateHolder.PartitionKeyData())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodePagingPredicateHolder(frameIterator *proto.ForwardFrameIterator) proto.PagingPredicateHolder {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	pageSize := FixSizedTypesCodec.DecodeInt(initialFrame.Content, PagingPredicateHolderCodecPageSizeFieldOffset)
	page := FixSizedTypesCodec.DecodeInt(initialFrame.Content, PagingPredicateHolderCodecPageFieldOffset)
	iterationTypeId := FixSizedTypesCodec.DecodeByte(initialFrame.Content, PagingPredicateHolderCodecIterationTypeIdFieldOffset)

	anchorDataListHolder := DecodeAnchorDataListHolder(frameIterator)
	predicateData := CodecUtil.DecodeNullableForData(frameIterator)
	comparatorData := CodecUtil.DecodeNullableForData(frameIterator)
	partitionKeyData := CodecUtil.DecodeNullableForData(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return proto.NewPagingPredicateHolder(anchorDataListHolder, predicateData, comparatorData, pageSize, page, iterationTypeId, partitionKeyData)
}
