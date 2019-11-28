/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package proto

import (
"bytes"
"github.com/hazelcast/hazelcast-go-client/serialization"
_ "github.com/hazelcast/hazelcast-go-client"
)

type ErrorHolder struct {
errorCode int
className string
message string
stackTraceElements []StackTraceElement
}

//@Generated("ba73e77eab5ba05ffae40e0802c4317f")
const (
    ErrorHolderErrorCodeFieldOffset = 0
    ErrorHolderInitialFrameSize = ErrorHolderErrorCodeFieldOffset + bufutil.IntSizeInBytes
)

func ErrorHolderEncode(clientMessage bufutil.ClientMessagex, errorHolder ErrorHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, ErrorHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset, errorHolder.errorCode)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, errorHolder.className)
        CodecUtil.encodeNullable(clientMessage, errorHolder.message, String.encode)
        ListMultiFrameCodec.encode(clientMessage, errorHolder.getStackTraceElements(), StackTraceElement.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func ErrorHolderDecode(iterator bufutil.ClientMessagex)  *ErrorHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        errorCode := bufutil.DecodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset)
        className := StringCodec.decode(iterator)
        message := CodecUtil.decodeNullable(iterator, String.decode)
        stackTraceElements := ListMultiFrameCodec.decode(iterator, StackTraceElement.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &ErrorHolder { errorCode, className, message, stackTraceElements }
    }