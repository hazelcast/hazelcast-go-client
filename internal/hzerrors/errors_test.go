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

package hzerrors_test

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

func TestClientError_Is(t *testing.T) {
	err := ihzerrors.NewIOError("some io error", &os.PathError{
		Op:   "Open",
		Path: "/foo/bar",
		Err:  os.ErrNotExist,
	})
	assert.True(t, errors.Is(err, hzerrors.ErrIO))
	assert.Equal(t, "some io error: Open /foo/bar: file does not exist", err.Error())
	var pathErr *os.PathError
	assert.True(t, errors.As(err, &pathErr))
}
