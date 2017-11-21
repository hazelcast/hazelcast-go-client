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

package common

type HazelcastError interface {
	Error() string
	Cause() error
}

type HazelcastErrorType struct {
	message string
	cause   error
}

func (e *HazelcastErrorType) Error() string {
	return e.message
}

func (e *HazelcastErrorType) Cause() error {
	return e.cause
}

type HazelcastEOFError struct {
	*HazelcastErrorType
}

type HazelcastSerializationError struct {
	*HazelcastErrorType
}
type HazelcastTimeoutError struct {
	*HazelcastErrorType
}
type HazelcastAuthenticationError struct {
	*HazelcastErrorType
}

type HazelcastIllegalArgumentError struct {
	*HazelcastErrorType
}
type HazelcastClientNotActiveError struct {
	*HazelcastErrorType
}
type HazelcastTargetNotMemberError struct {
	*HazelcastErrorType
}
type HazelcastIllegalStateError struct {
	*HazelcastErrorType
}
type HazelcastTargetDisconnectedError struct {
	*HazelcastErrorType
}
type HazelcastInstanceNotActiveError struct {
	*HazelcastErrorType
}
type HazelcastIOError struct {
	*HazelcastErrorType
}

func NewHazelcastTimeoutError(message string, cause error) *HazelcastTimeoutError {
	return &HazelcastTimeoutError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastIOError(message string, cause error) *HazelcastIOError {
	return &HazelcastIOError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastClientNotActiveError(message string, cause error) *HazelcastClientNotActiveError {
	return &HazelcastClientNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastErrorType(message string, cause error) *HazelcastErrorType {
	return &HazelcastErrorType{message: message, cause: cause}
}
func NewHazelcastIllegalStateError(message string, cause error) *HazelcastIllegalStateError {
	return &HazelcastIllegalStateError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastTargetDisconnectedError(message string, cause error) *HazelcastTargetDisconnectedError {
	return &HazelcastTargetDisconnectedError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastEOFError(message string, cause error) *HazelcastEOFError {
	return &HazelcastEOFError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastSerializationError(message string, cause error) *HazelcastSerializationError {
	return &HazelcastSerializationError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastIllegalArgumentError(message string, cause error) *HazelcastIllegalArgumentError {
	return &HazelcastIllegalArgumentError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastAuthenticationError(message string, cause error) *HazelcastAuthenticationError {
	return &HazelcastAuthenticationError{&HazelcastErrorType{message, cause}}
}
