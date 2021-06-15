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

package rest

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type Error struct {
	Text string
	Code int
}

func NewError(code int, text string) *Error {
	return &Error{
		Code: code,
		Text: text,
	}
}

func NewErrorFromResponse(resp *http.Response) *Error {
	code := resp.StatusCode
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return NewError(code, "(cannot read error message)")
	}
	text := string(body)
	// error is unhandled
	resp.Body.Close()
	return NewError(code, text)
}

func (e Error) Error() string {
	return fmt.Sprintf("HTTP error: %d, %s", e.Code, e.Text)
}
