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

package validationutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateAsNonNegativeInt32(t *testing.T) {
	testCases := []struct {
		value         int
		expectedValue int32
	}{
		{0, int32(0)},
		{42, int32(42)},
		{math.MaxInt32, int32(math.MaxInt32)},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			val, err := ValidateAsNonNegativeInt32(tc.value)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedValue, val)
		})
	}
}

func TestValidateAsNonNegativeInt32_Error(t *testing.T) {
	testCases := []struct {
		value          int
		expectedErrMsg string
	}{
		{-1, "non-negative"},
		{math.MaxInt32 + 1, "32-bit"},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			_, err := ValidateAsNonNegativeInt32(tc.value)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrMsg)
		})
	}
}
