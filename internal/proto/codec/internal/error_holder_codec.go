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

const (
	ErrorHolderCodecErrorCodeFieldOffset      = 0
	ErrorHolderCodecErrorCodeInitialFrameSize = ErrorHolderCodecErrorCodeFieldOffset + proto.IntSizeInBytes
)

type errorholderCodec struct{}

var ErrorHolderCodec errorholderCodec

func (errorholderCodec) Encode(clientMessage *proto.ClientMessage, errorHolder proto.ErrorHolder) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, ErrorHolderCodecErrorCodeInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ErrorHolderCodecErrorCodeFieldOffset, errorHolder.GetErrorCode())
	clientMessage.AddFrame(initialFrame)

	StringCodec.Encode(clientMessage, errorHolder.GetClassName())
	CodecUtil.EncodeNullableForString(clientMessage, errorHolder.GetMessage())
	ListMultiFrameCodec.EncodeForStackTraceElement(clientMessage, errorHolder.GetStackTraceElements())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (errorholderCodec) Decode(frameIterator *proto.ForwardFrameIterator) proto.ErrorHolder {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	errorCode := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ErrorHolderCodecErrorCodeFieldOffset)

	className := StringCodec.Decode(frameIterator)
	message := CodecUtil.DecodeNullableForString(frameIterator)
	stackTraceElements := ListMultiFrameCodec.DecodeForStackTraceElement(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return proto.NewErrorHolder(errorCode, className, message, stackTraceElements)
}
