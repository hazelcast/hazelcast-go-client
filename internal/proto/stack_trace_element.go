/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

type StackTraceElement struct {
	className  string
	methodName string
	fileName   string
	lineNumber int32
}

func NewStackTraceElement(className, methodName, fileName string, lineNumber int32) StackTraceElement {
	return StackTraceElement{className, methodName, fileName, lineNumber}
}

func (s StackTraceElement) ClassName() string {
	return s.className
}

func (s StackTraceElement) MethodName() string {
	return s.methodName
}

func (s StackTraceElement) FileName() string {
	return s.fileName
}

func (s StackTraceElement) LineNumber() int32 {
	return s.lineNumber
}
