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

package types

import (
	"fmt"
	"math"
	"math/big"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
)

// Decimal is a wrapper for Hazelcast Decimal.
type Decimal struct {
	unscaledValue *big.Int
	scale         int32
}

// NewDecimal creates and returns a Decimal value with the given big int and scale.
// Scale must be nonnegative and must be less or equal to math.MaxInt32. otherwise NewDecimal panics.
func NewDecimal(unscaledValue *big.Int, scale int) Decimal {
	if err := check.WithinRangeInt32(int32(scale), 0, math.MaxInt32); err != nil {
		panic(fmt.Errorf("creating decumal: %w", err))
	}
	return Decimal{
		unscaledValue: unscaledValue,
		scale:         int32(scale),
	}
}

// UnscaledValue returns the unscaled value of the decimal.
func (d Decimal) UnscaledValue() *big.Int {
	return d.unscaledValue
}

// Scale returns the scale of the decimal.
// The returned value is nonnegative and less or equal to math.MaxInt32.
func (d Decimal) Scale() int {
	return int(d.scale)
}

// TODO: String method
