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

package check

import (
	"fmt"
	"math"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

func NonNegativeInt32(n int) (int32, error) {
	if err := ensureNonNegativeInt32(n, hzerrors.ErrIllegalArgument); err != nil {
		return 0, err
	}
	return int32(n), nil
}

func EnsureNonNegativeDuration(v *time.Duration, d time.Duration, msg string) error {
	if *v < 0 {
		return fmt.Errorf("%s: %w", msg, hzerrors.ErrIllegalArgument)
	}
	if *v == 0 {
		*v = d
	}
	return nil
}

func WithinRangeInt32(n, start, end int32) error {
	if n < start || n > end {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("number %d is out of range [%d,%d]", n, start, end), nil)
	}
	return nil
}

func NonNegativeInt32Config(n int) error {
	return ensureNonNegativeInt32(n, hzerrors.ErrInvalidConfiguration)
}

func NonNegativeInt64Config(n int64) error {
	return ensureNonNegativeInt64(n, hzerrors.ErrInvalidConfiguration)
}

func ensureNonNegativeInt32(n int, err error) error {
	if n < 0 {
		return fmt.Errorf("non-negative integer expected: %w", err)
	}
	if n > math.MaxInt32 {
		return fmt.Errorf("integer overflows int32: %w", err)
	}
	return nil
}

func ensureNonNegativeInt64(n int64, err error) error {
	if n < 0 {
		return fmt.Errorf("non-negative integer expected: %w", err)
	}
	return nil
}
