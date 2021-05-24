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
)

var (
	ErrClientOffline             = errors.New("client offline")
	ErrClientNotAllowedInCluster = errors.New("client not allowed in cluster")
	ErrAddressNotFound           = errors.New("address not found")
)

// HazelcastError is the general error interface.
type HazelcastError interface {
	// Error returns the error message.
	Error() string

	// Cause returns the cause of error.
	Cause() error

	// ServerError returns error info from server side.
	ServerError() *ServerError
}

// HazelcastErrorType is the general error struct.
type HazelcastErrorType struct {
	cause   error
	message string // cause is the cause error.
	// message is the error message.
}

// Error returns the error message.
func (e *HazelcastErrorType) Error() string {
	return e.message
}

// Cause returns the cause error.
func (e *HazelcastErrorType) Cause() error {
	return e.cause
}

// ServerError returns error info from server side.
// It checks if the cause implements ServerError and if it doesnt
// it return nil.
func (e *HazelcastErrorType) ServerError() *ServerError {
	serverError := &ServerError{}
	if errors.As(e.cause, &serverError) {
		return serverError
	}
	return nil
}

// HazelcastEOFError is returned when an EOF error occurs.
type HazelcastEOFError struct {
	*HazelcastErrorType
}

// HazelcastSerializationError is returned when an error occurs while serializing/deserializing objects.
type HazelcastSerializationError struct {
	*HazelcastErrorType
}

// HazelcastOperationTimeoutError is returned when an operation times out.
type HazelcastOperationTimeoutError struct {
	*HazelcastErrorType
}

// HazelcastAuthenticationError is returned when there is an Authentication failure, e.g., credentials from client is not valid.
type HazelcastAuthenticationError struct {
	*HazelcastErrorType
}

// HazelcastIllegalArgumentError is returned when a method has been passed an illegal or inappropriate argument.
type HazelcastIllegalArgumentError struct {
	*HazelcastErrorType
}

// HazelcastClientNotActiveError is returned when Hazelcast client is not active during an invocation.
type HazelcastClientNotActiveError struct {
	*HazelcastErrorType
}

// HazelcastTargetNotMemberError indicates operation is send to a machine that isn't member of the cluster.
type HazelcastTargetNotMemberError struct {
	*HazelcastErrorType
}

// HazelcastIllegalStateError is returned when a method has been invoked at an illegal or inappropriate time.
type HazelcastIllegalStateError struct {
	*HazelcastErrorType
}

// HazelcastNilPointerError is returned when a nil argument has been passed to a method.
type HazelcastNilPointerError struct {
	*HazelcastErrorType
}

// HazelcastTargetDisconnectedError indicates that an operation is about to be sent to a non existing machine.
type HazelcastTargetDisconnectedError struct {
	*HazelcastErrorType
}

// HazelcastInstanceNotActiveError is returned when HazelcastInstance is not active during an invocation.
type HazelcastInstanceNotActiveError struct {
	*HazelcastErrorType
}

// HazelcastIOError is returned when an IO error occurs.
type HazelcastIOError struct {
	*HazelcastErrorType
}

// HazelcastNoDataMemberInClusterError is returned when there is no data member in the cluster to assign partitions.
type HazelcastNoDataMemberInClusterError struct {
	*HazelcastErrorType
}

// HazelcastClientServiceNotFoundError indicates that a requested client service doesn't exist.
type HazelcastClientServiceNotFoundError struct {
	*HazelcastErrorType
}

// HazelcastUnsupportedOperationError is returned to indicate that the requested operation is not supported.
type HazelcastUnsupportedOperationError struct {
	*HazelcastErrorType
}

// HazelcastCertificateError is returned when there is an error in certificates.
type HazelcastCertificateError struct {
	*HazelcastErrorType
}

// HazelcastConsistencyLostError is an error that indicates that the consistency guarantees provided by
// some service has been lost. The exact guarantees depend on the service.
type HazelcastConsistencyLostError struct {
	*HazelcastErrorType
}

// HazelcastTopicOverflowError is returned when a publisher wants to write to a topic, but there is not sufficient storage
// to deal with the event.
// This is returned only when reliable topic is used.
type HazelcastTopicOverflowError struct {
	*HazelcastErrorType
}

// NewHazelcastTopicOverflowError return a HazelcastTopicOverflowError.
func NewHazelcastTopicOverflowError(message string, cause error) *HazelcastTopicOverflowError {
	return &HazelcastTopicOverflowError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastNilPointerError returns a HazelcastNilPointerError.
func NewHazelcastNilPointerError(message string, cause error) *HazelcastNilPointerError {
	return &HazelcastNilPointerError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastIOError returns a HazelcastIOError.
func NewHazelcastIOError(message string, cause error) *HazelcastIOError {
	return &HazelcastIOError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastClientNotActiveError returns a HazelcastClientNotActiveError.
func NewHazelcastClientNotActiveError(message string, cause error) *HazelcastClientNotActiveError {
	return &HazelcastClientNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastErrorType returns a HazelcastErrorType.
func NewHazelcastErrorType(message string, cause error) *HazelcastErrorType {
	return &HazelcastErrorType{message: message, cause: cause}
}

// NewHazelcastIllegalStateError returns a HazelcastIllegalStateError.
func NewHazelcastIllegalStateError(message string, cause error) *HazelcastIllegalStateError {
	return &HazelcastIllegalStateError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastTargetDisconnectedError returns a HazelcastTargetDisconnectedError.
func NewHazelcastTargetDisconnectedError(message string, cause error) *HazelcastTargetDisconnectedError {
	return &HazelcastTargetDisconnectedError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastEOFError returns a HazelcastEOFError.
func NewHazelcastEOFError(message string, cause error) *HazelcastEOFError {
	return &HazelcastEOFError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastSerializationError returns a HazelcastSerializationError.
func NewHazelcastSerializationError(message string, cause error) *HazelcastSerializationError {
	return &HazelcastSerializationError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastIllegalArgumentError returns a HazelcastIllegalArgumentError.
func NewHazelcastIllegalArgumentError(message string, cause error) *HazelcastIllegalArgumentError {
	return &HazelcastIllegalArgumentError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastAuthenticationError returns a HazelcastAuthenticationError.
func NewHazelcastAuthenticationError(message string, cause error) *HazelcastAuthenticationError {
	return &HazelcastAuthenticationError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastOperationTimeoutError returns a HazelcastOperationTimeoutError.
func NewHazelcastOperationTimeoutError(message string, cause error) *HazelcastOperationTimeoutError {
	return &HazelcastOperationTimeoutError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastInstanceNotActiveError returns a HazelcastInstanceNotActiveError.
func NewHazelcastInstanceNotActiveError(message string, cause error) *HazelcastInstanceNotActiveError {
	return &HazelcastInstanceNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastTargetNotMemberError returns a HazelcastTargetNotMemberError.
func NewHazelcastTargetNotMemberError(message string, cause error) *HazelcastTargetNotMemberError {
	return &HazelcastTargetNotMemberError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastNoDataMemberInClusterError returns a HazelcastNoDataMemberInClusterError.
func NewHazelcastNoDataMemberInClusterError(message string, cause error) *HazelcastNoDataMemberInClusterError {
	return &HazelcastNoDataMemberInClusterError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastUnsupportedOperationError returns a HazelcastUnsupportedOperationError.
func NewHazelcastUnsupportedOperationError(message string, cause error) *HazelcastUnsupportedOperationError {
	return &HazelcastUnsupportedOperationError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastConsistencyLostError returns a HazelcastConsistencyLostError.
func NewHazelcastConsistencyLostError(message string, cause error) *HazelcastConsistencyLostError {
	return &HazelcastConsistencyLostError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastCertificateError returns a HazelcastCertificateError.
func NewHazelcastCertificateError(message string, cause error) *HazelcastCertificateError {
	return &HazelcastCertificateError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastClientServiceNotFoundError returns a HazelcastClientServiceNotFoundError.
func NewHazelcastClientServiceNotFoundError(message string, cause error) *HazelcastClientServiceNotFoundError {
	return &HazelcastClientServiceNotFoundError{&HazelcastErrorType{
		message: message, cause: cause,
	}}
}

// StackTraceElement contains stacktrace information for server side exception.
type StackTraceElement interface {
	// ClassName returns the fully qualified name of the class containing
	// the execution point represented by the stack trace element.
	ClassName() string

	// MethodName returns the name of the method containing the execution point
	// represented by this stack trace element.
	MethodName() string

	// FileName returns the name of the file containing the execution point
	// represented by the stack trace element, or nil if
	// this information is unavailable.
	FileName() string

	// LineNumber returns the line number of the source line containing the
	// execution point represented by this stack trace element, or
	// a negative number if this information is unavailable. A value
	// of -2 indicates that the method containing the execution point
	// is a native method.
	LineNumber() int32
}

func NewHazelcastError(err *ServerError) HazelcastError {
	sb := strings.Builder{}
	for _, trace := range err.StackTrace() {
		sb.WriteString(fmt.Sprintf("\n %s.%s(%s:%d)", trace.ClassName(), trace.MethodName(), trace.FileName(), trace.LineNumber()))
	}
	message := fmt.Sprintf("got exception from server:\n %s: %s\n %s", err.ClassName(), err.Message(), sb.String())
	switch errorCode(err.ErrorCode()) {
	case errorCodeAuthentication:
		return NewHazelcastAuthenticationError(message, err)
	case errorCodeHazelcastInstanceNotActive:
		return NewHazelcastInstanceNotActiveError(message, err)
	case errorCodeHazelcastSerialization:
		return NewHazelcastSerializationError(message, err)
	case errorCodeTargetDisconnected:
		return NewHazelcastTargetDisconnectedError(message, err)
	case errorCodeTargetNotMember:
		return NewHazelcastTargetNotMemberError(message, err)
	case errorCodeUnsupportedOperation:
		return NewHazelcastUnsupportedOperationError(message, err)
	case errorCodeConsistencyLostException:
		return NewHazelcastConsistencyLostError(message, err)
	case errorCodeIllegalArgument:
		return NewHazelcastIllegalArgumentError(message, err)
	}
	return NewHazelcastErrorType(message, err)
}

func MakeError(rec interface{}) error {
	switch v := rec.(type) {
	case error:
		return v
	case string:
		return errors.New(v)
	default:
		return fmt.Errorf("%v", rec)
	}
}
