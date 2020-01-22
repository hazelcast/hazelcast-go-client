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
    "github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type ErrorHolder struct {
errorCode int32
className string
message string
stackTraceElements []StackTraceElement
}

//@Generated("f2ddb9e11fa8e02631ad78da9d76b24c")
const (
    ErrorHolderErrorCodeFieldOffset = 0
    ErrorHolderInitialFrameSize = ErrorHolderErrorCodeFieldOffset + bufutil.IntSizeInBytes
)

func ErrorHolderCodecEncode(clientMessage *bufutil.ClientMessage, errorHolder ErrorHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := &bufutil.Frame{Content: make([]byte, ErrorHolderInitialFrameSize), Flags: bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset, errorHolder.errorCode)
        clientMessage.Add(initialFrame)
        bufutil.StringCodecEncode(clientMessage, errorHolder.className)
        bufutil.EncodeNullable(clientMessage, errorHolder.message, bufutil.StringCodecEncode)
        elements :=  errorHolder.stackTraceElements
         clientMessage.Add(bufutil.BeginFrame)
        for i := 0; i < len(elements) ; i++ {
            StackTraceElementCodecEncode(clientMessage, elements[i])
        }

        clientMessage.Add(bufutil.EndFrame)
    }

func ErrorHolderCodecDecode(iterator *bufutil.ForwardFrameIterator)  ErrorHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        errorCode := bufutil.DecodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset)
        className := bufutil.StringCodecDecode(iterator)
        message := bufutil.DecodeNullable(iterator, bufutil.StringCodecDecode).(string)
        var stackTraceElements []StackTraceElement
         //begin frame, list
        iterator.Next()
        for !bufutil.NextFrameIsDataStructureEndFrame(iterator) {
            stackTraceElements = append(stackTraceElements, StackTraceElementCodecDecode(iterator))
        }
        //end frame, list
        iterator.Next()
        bufutil.FastForwardToEndFrame(iterator)
        return ErrorHolder { errorCode, className, message, stackTraceElements }
    }