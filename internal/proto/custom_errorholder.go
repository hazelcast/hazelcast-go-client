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
)

type ErrorHolder struct {
errorCode int32
className string
message string
stackTraceElements []StackTraceElement
}

//CONSTRUCTOR
func NewErrorHolder(errorCode int32,className string,message string,stackTraceElements []StackTraceElement) *ErrorHolder {
return &ErrorHolder{errorCode,className,message,stackTraceElements}
}


//GETTERS
func (x *ErrorHolder) ErrorCode() int32 {
    return x.errorCode
    }
func (x *ErrorHolder) ClassName() string {
    return x.className
    }
func (x *ErrorHolder) Message() string {
    return x.message
    }
func (x *ErrorHolder) StackTraceElements() []StackTraceElement {
    return x.stackTraceElements
    }


//@Generated("587b4b4e842dbecb9d77f41455cb8463")
const (
    ErrorHolderErrorCodeFieldOffset = 0
    ErrorHolderInitialFrameSize = ErrorHolderErrorCodeFieldOffset + IntSizeInBytes
)

func ErrorHolderCodecEncode(clientMessage *ClientMessage, errorHolder ErrorHolder) {
        clientMessage.Add(BeginFrame)
        initialFrame := &Frame{Content: make([]byte, ErrorHolderInitialFrameSize), Flags: UnfragmentedMessage}
        EncodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset, errorHolder.errorCode)
        clientMessage.Add(initialFrame)
        StringCodecEncode(clientMessage, errorHolder.className)
        EncodeNullable(clientMessage, errorHolder.message, StringCodecEncode)
        elements :=  errorHolder.stackTraceElements
         clientMessage.Add(BeginFrame)
        for i := 0; i < len(elements) ; i++ {
            StackTraceElementCodecEncode(clientMessage, elements[i])
        }

        clientMessage.Add(EndFrame)
    }

func ErrorHolderCodecDecode(iterator *ForwardFrameIterator)  ErrorHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        errorCode := DecodeInt(initialFrame.Content, ErrorHolderErrorCodeFieldOffset)
        className := StringCodecDecode(iterator)
        message := DecodeNullable(iterator, StringCodecDecode).(string)
        var stackTraceElements []StackTraceElement
         //begin frame, list
        iterator.Next()
        for !NextFrameIsDataStructureEndFrame(iterator) {
            stackTraceElements = append(stackTraceElements, StackTraceElementCodecDecode(iterator))
        }
        //end frame, list
        iterator.Next()
        FastForwardToEndFrame(iterator)
        return ErrorHolder { errorCode, className, message, stackTraceElements }
    }