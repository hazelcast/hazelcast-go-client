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

package hzerrors

type ServerError struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []StackTraceElement
	causeErrorCode int32
	causeClassName string
}

// NewServerErrorImpl
// experimental
func NewServerErrorImpl(errorCode int32, className string, message string, stackTrace []StackTraceElement, causeErrorCode int32, causeClassName string) ServerError {
	return ServerError{
		errorCode:      errorCode,
		className:      className,
		message:        message,
		stackTrace:     stackTrace,
		causeErrorCode: causeErrorCode,
		causeClassName: causeClassName,
	}
}

func (e *ServerError) Error() string {
	return e.message
}

func (e *ServerError) ErrorCode() int32 {
	return e.errorCode
}

func (e *ServerError) ClassName() string {
	return e.className
}

func (e *ServerError) Message() string {
	return e.message
}

func (e *ServerError) StackTrace() []StackTraceElement {
	stackTrace := make([]StackTraceElement, len(e.stackTrace))
	for i, v := range e.stackTrace {
		stackTrace[i] = v
	}
	return stackTrace
}

func (e *ServerError) CauseErrorCode() int32 {
	return e.causeErrorCode
}

func (e *ServerError) CauseClassName() string {
	return e.causeClassName
}
