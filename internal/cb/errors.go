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

package cb

import "errors"

var ErrCircuitOpen = errors.New("circuit open")

type NonRetryableError struct {
	Err error
}

func WrapNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{err}
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
