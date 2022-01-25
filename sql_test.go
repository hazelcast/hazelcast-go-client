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

package hazelcast

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/it/runtime"
)

type cursorBufferSizeTestCase struct {
	Value     int
	Target    *int32
	ErrString string
}

func TestSQLOptions_SetCursorBufferSize(t *testing.T) {
	v0 := int32(0)
	v1 := int32(1)
	v4096 := int32(4096)
	testCases := []cursorBufferSizeTestCase{
		{Value: 0, Target: &v0},
		{Value: 1, Target: &v1},
		{Value: 4096, Target: &v4096},
		{Value: -1, ErrString: "setting cursor buffer size: non-negative integer number expected: -1: illegal argument error"},
	}
	if !runtime.Is32BitArch() {
		v := math.MaxInt32
		testCases = append(testCases, cursorBufferSizeTestCase{
			Value:     v + 1,
			ErrString: "setting cursor buffer size: signed 32-bit integer number expected: 2147483648: illegal argument error",
		})
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(tc.Value), func(t *testing.T) {
			opts := SQLOptions{}
			opts.SetCursorBufferSize(tc.Value)
			err := opts.validate()
			if tc.ErrString != "" {
				assert.Equal(t, tc.ErrString, err.Error())
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.Target, opts.cursorBufferSize)
		})
	}
}

func TestSQLOptions_SetQueryTimeout(t *testing.T) {
	v := int64(0)
	v1 := int64(-1)
	v5000 := int64(5000)
	testCases := []struct {
		V time.Duration
		T *int64
	}{
		{V: 0, T: &v},
		{V: -1, T: &v1},
		{V: -100, T: &v1},
		{V: 5 * time.Second, T: &v5000},
	}
	for _, tc := range testCases {
		t.Run(tc.V.String(), func(t *testing.T) {
			opts := SQLOptions{}
			if err := opts.validate(); err != nil {
				t.Fatal(err)
			}
			opts.SetQueryTimeout(tc.V)
			assert.Equal(t, tc.T, opts.timeout)
		})
	}
}

func TestSQLOptions_SetSchema(t *testing.T) {
	blank := ""
	foo := "foo"
	testCases := []struct {
		V string
		T *string
	}{
		{V: "", T: &blank},
		{V: "foo", T: &foo},
	}
	for _, tc := range testCases {
		t.Run(tc.V, func(t *testing.T) {
			opts := SQLOptions{}
			if err := opts.validate(); err != nil {
				t.Fatal(err)
			}
			opts.SetSchema(tc.V)
			assert.Equal(t, tc.T, opts.schema)
		})
	}
}
