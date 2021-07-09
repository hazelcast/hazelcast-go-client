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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
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
		msg   string
		value int
	}{
		{"non-negative", -1},
		{"32-bit", math.MaxInt32 + 1},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			_, err := ValidateAsNonNegativeInt32(tc.value)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}
}

func TestNonNegativeDuration(t *testing.T) {
	v := types.Duration(-1)
	if err := NonNegativeDuration(&v, 5*time.Second, "invalid"); !errors.Is(err, hzerrors.ErrIllegalArgument) {
		t.Fatalf("unexpected error")
	}
	v = types.Duration(0)
	if err := NonNegativeDuration(&v, 5*time.Second, "invalid"); err != nil {
		t.Fatalf("unexpected error")
	}
	assert.Equal(t, types.Duration(5*time.Second), v)
}
