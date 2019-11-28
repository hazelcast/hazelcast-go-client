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

type StackTraceElement struct {
className string
methodName string
fileName string
lineNumber int
}

//@Generated("3b6699805f434c0b91814fcb5371e950")
const (
    StackTraceElementLineNumberFieldOffset = 0
    StackTraceElementInitialFrameSize = StackTraceElementLineNumberFieldOffset + bufutil.IntSizeInBytes
)

func StackTraceElementEncode(clientMessage bufutil.ClientMessagex, stackTraceElement StackTraceElement) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, StackTraceElementInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, StackTraceElementLineNumberFieldOffset, stackTraceElement.lineNumber)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, stackTraceElement.className)
        StringCodec.encode(clientMessage, stackTraceElement.methodName)
        CodecUtil.encodeNullable(clientMessage, stackTraceElement.fileName, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func StackTraceElementDecode(iterator bufutil.ClientMessagex)  *StackTraceElement  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        lineNumber := bufutil.DecodeInt(initialFrame.Content, StackTraceElementLineNumberFieldOffset)
        className := StringCodec.decode(iterator)
        methodName := StringCodec.decode(iterator)
        fileName := CodecUtil.decodeNullable(iterator, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &StackTraceElement { className, methodName, fileName, lineNumber }
    }