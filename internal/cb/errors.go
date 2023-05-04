/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package cb

import "errors"

var ErrCircuitOpen = errors.New("circuit open")
var ErrDeadlineExceeded = errors.New("deadline exceeded")

type NonRetryableError struct {
	// Err is the wrapper error
	Err error
	// Persistent disables unwrapping the error in the CB if true
	// This is useful to make the error non-retryable in chain of Trys
	Persistent bool
}

func WrapNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}

func WrapNonRetryableErrorPersistent(err error) *NonRetryableError {
	return &NonRetryableError{
		Err:        err,
		Persistent: true,
	}
}

func (n NonRetryableError) Error() string {
	return n.Err.Error()
}

func (e NonRetryableError) Is(target error) bool {
	if _, ok := target.(*NonRetryableError); ok {
		return true
	}
	_, ok := target.(NonRetryableError)
	return ok
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}
