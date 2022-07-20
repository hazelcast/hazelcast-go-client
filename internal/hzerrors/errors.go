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

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

// StackTraceElement contains stacktrace information for server side exception.
type StackTraceElement struct {
	// ClassName is the fully qualified name of the class containing the execution point represented by the stack trace element.
	ClassName string
	// MethodName is the name of the method containing the execution point represented by this stack trace element.
	MethodName string
	// FileName returns the name of the file containing the execution point represented by the stack trace element,
	FileName string
	// LineNumber returns the line number of the source line containing the execution point represented by this stack trace element,
	// or a negative number if this information is unavailable
	// A value of -2 indicates that the method containing the execution point is a native method.
	LineNumber int32
}

type ServerError struct {
	ClassName      string
	Message        string
	CauseClassName string
	StackTrace     []StackTraceElement
	ErrorCode      int32
	CauseErrorCode int32
}

func NewServerError(errorCode int32, className string, message string, stackTrace []StackTraceElement) *ServerError {
	return &ServerError{
		ErrorCode:  errorCode,
		ClassName:  className,
		Message:    message,
		StackTrace: stackTrace,
	}
}

func (e *ServerError) Error() string {
	return e.Message
}

func (e *ServerError) String() string {
	sb := strings.Builder{}
	for _, trace := range e.StackTrace {
		sb.WriteString(fmt.Sprintf("\n %s.%s(%s:%d)", trace.ClassName, trace.MethodName, trace.FileName, trace.LineNumber))
	}
	return fmt.Sprintf("got exception from server:\n %s: %s\n %s", e.ClassName, e.Message, sb.String())
}

type ClientError struct {
	WrappedErr error
	Err        error
	Message    string
}

func NewClientError(msg string, wrapped, err error) *ClientError {
	return &ClientError{
		Message:    msg,
		WrappedErr: wrapped,
		Err:        err,
	}
}

func (e ClientError) Error() string {
	msg := ""
	if e.Message != "" {
		msg = fmt.Sprintf("%s: ", e.Message)
	}
	if e.WrappedErr != nil {
		return fmt.Sprintf("%s%s", msg, e.WrappedErr.Error())
	}
	if e.Err != nil {
		return fmt.Sprintf("%s%s", msg, e.Err.Error())
	}
	return e.Message
}

func (e ClientError) Unwrap() error {
	return e.WrappedErr
}

func (e ClientError) Wrap(err error) error {
	eCopy := e
	eCopy.WrappedErr = err
	return fmt.Errorf("%s: %w", e.Message, err)
}

func (e ClientError) Is(target error) bool {
	if e.Err == nil {
		return false
	}
	return errors.Is(target, e.Err)
}

func (c ClientError) IsRetryable() bool {
	// c.Err is supposed to be a concrete error
	// we don't want to unwrap it, so it's OK to do a type check
	_, ok := c.Err.(*hzerrors.RetryableError)
	return ok
}

func NewIllegalArgumentError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrIllegalArgument)
}

func NewSerializationError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrHazelcastSerialization)
}

func NewIOError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrIO)
}

func NewEOFError(msg string) *ClientError {
	return NewClientError(msg, nil, hzerrors.ErrEOF)
}

func NewTargetDisconnectedError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrTargetDisconnected)
}

func NewInstanceNotActiveError(msg string) *ClientError {
	return NewClientError(msg, nil, hzerrors.ErrHazelcastInstanceNotActive)
}

func NewIllegalStateError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrIllegalState)
}

func NewSQLError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrSQL)
}

func NewInvalidConfigurationError(msg string, err error) *ClientError {
	return NewClientError(msg, err, hzerrors.ErrInvalidConfiguration)
}

func IsRetryable(err error) bool {
	// check whether the error is retryable
	if _, ok := err.(*hzerrors.RetryableError); ok {
		return true
	}
	if c, ok := err.(*ClientError); ok {
		if c.IsRetryable() {
			return true
		}
	}
	return false
}
