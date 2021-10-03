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
	"fmt"
	"math"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	intSize = 32 << (^uint(0) >> 63) // 32 or 64
	MaxInt  = 1<<(intSize-1) - 1
)

func ValidateAsNonNegativeInt32(n int) (int32, error) {
	if n < 0 {
		return 0, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("non-negative integer number expected: %d", n), nil)
	}
	if n > math.MaxInt32 {
		return 0, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("signed 32-bit integer number expected: %d", n), nil)
	}
	return int32(n), nil
}

func WithinRangeInt32(n, start, end int32) error {
	if n < start || n > end {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("number %d is out of range [%d,%d]", n, start, end), nil)
	}
	return nil
}

func NonNegativeDuration(v *types.Duration, d time.Duration, msg string) error {
	if *v < 0 {
		return fmt.Errorf("%s: %w", msg, hzerrors.ErrIllegalArgument)
	}
	if *v == 0 {
		*v = types.Duration(d)
	}
	return nil
}
