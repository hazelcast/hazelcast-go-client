// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

// General HazelcastError interface.
type HazelcastError interface {
	// Error returns the error message.
	Error() string

	// cause returns the cause error.
	Cause() error
}

// General HazelcastErrorType struct.
type HazelcastErrorType struct {
	// message is the error message.
	message string

	// cause is the cause error.
	cause error
}

// Error returns the error message.
func (e *HazelcastErrorType) Error() string {
	return e.message
}

// Cause returns the cause error.
func (e *HazelcastErrorType) Cause() error {
	return e.cause
}

// HazelcastEOFError is returned when an EOF error occurs.
type HazelcastEOFError struct {
	*HazelcastErrorType
}

// HazelcastSerializationError is returned when an error occurs while serializing/deserializing objects.
type HazelcastSerializationError struct {
	*HazelcastErrorType
}

// HazelcastTimeoutError is returned when an operation times out.
type HazelcastTimeoutError struct {
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

// HazelcastIllegalStateError returns HazelcastIOError.
func NewHazelcastIOError(message string, cause error) *HazelcastIOError {
	return &HazelcastIOError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastClientNotActiveError returns HazelcastClientNotActiveError.
func NewHazelcastClientNotActiveError(message string, cause error) *HazelcastClientNotActiveError {
	return &HazelcastClientNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastErrorType returns HazelcastErrorType.
func NewHazelcastErrorType(message string, cause error) *HazelcastErrorType {
	return &HazelcastErrorType{message: message, cause: cause}
}

// NewHazelcastIllegalStateError returns HazelcastIllegalStateError.
func NewHazelcastIllegalStateError(message string, cause error) *HazelcastIllegalStateError {
	return &HazelcastIllegalStateError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastTargetDisconnectedError returns HazelcastTargetDisconnectedError.
func NewHazelcastTargetDisconnectedError(message string, cause error) *HazelcastTargetDisconnectedError {
	return &HazelcastTargetDisconnectedError{&HazelcastErrorType{message: message, cause: cause}}
}

// NewHazelcastEOFError returns HazelcastEOFError.
func NewHazelcastEOFError(message string, cause error) *HazelcastEOFError {
	return &HazelcastEOFError{&HazelcastErrorType{message, cause}}
}

// NewHazelcastSerializationError returns HazelcastSerializationError.
func NewHazelcastSerializationError(message string, cause error) *HazelcastSerializationError {
	return &HazelcastSerializationError{&HazelcastErrorType{message, cause}}
}

// NewHazelcastIllegalArgumentError returns HazelcastIllegalArgumentError.
func NewHazelcastIllegalArgumentError(message string, cause error) *HazelcastIllegalArgumentError {
	return &HazelcastIllegalArgumentError{&HazelcastErrorType{message, cause}}
}

// NewHazelcastAuthenticationError returns HazelcastAuthenticationError.
func NewHazelcastAuthenticationError(message string, cause error) *HazelcastAuthenticationError {
	return &HazelcastAuthenticationError{&HazelcastErrorType{message, cause}}
}

// NewHazelcastTimeoutError returns HazelcastTimeoutError.
func NewHazelcastTimeoutError(message string, cause error) *HazelcastTimeoutError {
	return &HazelcastTimeoutError{&HazelcastErrorType{message: message, cause: cause}}
}
