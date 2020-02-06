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

type StackTraceElement struct {
className string
methodName string
fileName string
lineNumber int32
}

//CONSTRUCTOR
func NewStackTraceElement(className string,methodName string,fileName string,lineNumber int32) *StackTraceElement {
return &StackTraceElement{className,methodName,fileName,lineNumber}
}


//GETTERS
func (x *StackTraceElement) ClassName() string {
    return x.className
    }
func (x *StackTraceElement) MethodName() string {
    return x.methodName
    }
func (x *StackTraceElement) FileName() string {
    return x.fileName
    }
func (x *StackTraceElement) LineNumber() int32 {
    return x.lineNumber
    }


//@Generated("55d34cca653bd7b2531113639ed29b84")
const (
    StackTraceElementLineNumberFieldOffset = 0
    StackTraceElementInitialFrameSize = StackTraceElementLineNumberFieldOffset + IntSizeInBytes
)

func StackTraceElementCodecEncode(clientMessage *ClientMessage, stackTraceElement StackTraceElement) {
        clientMessage.Add(BeginFrame)
        initialFrame := &Frame{Content: make([]byte, StackTraceElementInitialFrameSize), Flags: UnfragmentedMessage}
        EncodeInt(initialFrame.Content, StackTraceElementLineNumberFieldOffset, stackTraceElement.lineNumber)
        clientMessage.Add(initialFrame)
        StringCodecEncode(clientMessage, stackTraceElement.className)
        StringCodecEncode(clientMessage, stackTraceElement.methodName)
        EncodeNullable(clientMessage, stackTraceElement.fileName, StringCodecEncode)

        clientMessage.Add(EndFrame)
    }

func StackTraceElementCodecDecode(iterator *ForwardFrameIterator)  StackTraceElement  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        lineNumber := DecodeInt(initialFrame.Content, StackTraceElementLineNumberFieldOffset)
        className := StringCodecDecode(iterator)
        methodName := StringCodecDecode(iterator)
        fileName := DecodeNullable(iterator, StringCodecDecode).(string)
        FastForwardToEndFrame(iterator)
        return StackTraceElement { className, methodName, fileName, lineNumber }
    }