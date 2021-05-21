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

import "github.com/hazelcast/hazelcast-go-client/hzerrors"

type ErrorHolder struct {
	className          string
	message            string
	stackTraceElements []hzerrors.StackTraceElement
	errorCode          int32
}

func NewErrorHolder(errorCode int32, className, message string, stackTraceElements []hzerrors.StackTraceElement) ErrorHolder {
	return ErrorHolder{
		errorCode:          errorCode,
		className:          className,
		message:            message,
		stackTraceElements: stackTraceElements,
	}
}

func (e ErrorHolder) ErrorCode() int32 {
	return e.errorCode
}

func (e ErrorHolder) ClassName() string {
	return e.className
}

func (e ErrorHolder) Message() string {
	return e.message
}

func (e ErrorHolder) StackTraceElements() []hzerrors.StackTraceElement {
	return e.stackTraceElements
}
