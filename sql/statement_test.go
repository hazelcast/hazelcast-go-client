/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package sql

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/it/runtime"
)

type cursorBufferSizeTestCase struct {
	ErrString string
	Value     int
	Target    int32
}

func TestStatement_SetCursorBufferSize(t *testing.T) {
	v0 := int32(0)
	v1 := int32(1)
	v4096 := int32(4096)
	testCases := []cursorBufferSizeTestCase{
		{Value: 0, Target: v0},
		{Value: 1, Target: v1},
		{Value: 4096, Target: v4096},
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
			var stmt Statement
			err := stmt.SetCursorBufferSize(tc.Value)
			if tc.ErrString != "" {
				assert.Equal(t, tc.ErrString, err.Error())
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.Target, stmt.CursorBufferSize())
		})
	}
}

func TestStatement_SetQueryTimeout(t *testing.T) {
	v := int64(0)
	v1 := int64(-1)
	v5000 := int64(5000)
	testCases := []struct {
		V time.Duration
		T int64
	}{
		{V: 0, T: v},
		{V: -1, T: v1},
		{V: -100, T: v1},
		{V: 5 * time.Second, T: v5000},
	}
	for _, tc := range testCases {
		t.Run(tc.V.String(), func(t *testing.T) {
			var stmt Statement
			stmt.SetQueryTimeout(tc.V)
			assert.Equal(t, tc.T, stmt.QueryTimeout())
		})
	}
}

func TestStatement_SetSchema(t *testing.T) {
	blank := ""
	foo := "foo"
	testCases := []struct {
		V string
		T string
	}{
		{V: "", T: blank},
		{V: "foo", T: foo},
	}
	for _, tc := range testCases {
		t.Run(tc.V, func(t *testing.T) {
			var stmt Statement
			stmt.SetSchema(tc.V)
			assert.Equal(t, tc.T, stmt.Schema())
		})
	}
}

func TestStatement_DefaultValues(t *testing.T) {
	testParams := []interface{}{123, "test", []float32{12.012, 123.123}}
	testStatement := "sql test statement"
	stmt := NewStatement(testStatement, testParams...)
	assert.Equal(t, int64(-1), stmt.QueryTimeout())
	assert.Equal(t, ExpectedResultTypeAny, stmt.ExpectedResultType())
	assert.Equal(t, int32(4096), stmt.CursorBufferSize())
	assert.Equal(t, "", stmt.schema)
	assert.Equal(t, testStatement, stmt.SQL)
	assert.Equal(t, testParams, stmt.Parameters)
}
