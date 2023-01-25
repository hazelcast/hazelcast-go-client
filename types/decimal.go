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

package types

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

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

func (d Decimal) Float64() float64 {
	f, err := strconv.ParseFloat(d.String(), 64)
	if err != nil {
		return 0.0
	}
	return f
}

func (d Decimal) String() string {
	bigStr := d.unscaledValue.String()
	if d.scale == 0 {
		return bigStr
	}
	var negative bool
	point := int32(len(bigStr)) - d.scale
	if bigStr[0] == '-' {
		negative = true
		point -= 1
	} else {
		negative = false
	}
	var val strings.Builder
	if d.scale >= 0 && point-1 >= -6 {
		if point <= 0 {
			val.WriteString("0.")
			for point < 0 {
				val.WriteByte('0')
				point++
			}
			if negative {
				point++
			}
			val.WriteString(bigStr[point:])
		} else {
			if negative {
				point += 1
			}
			val.WriteString(bigStr[0:point] + "." + bigStr[point:])
		}
	} else {
		if len(bigStr) > 1 {
			if negative {
				val.WriteString(bigStr[0:2] + "." + bigStr[2:])
			} else {
				val.WriteString(bigStr[0:1] + "." + bigStr[2:])
			}
		}
		val.WriteByte('E')
		if point >= 0 {
			val.WriteByte('+')
		}
		val.WriteString(strconv.FormatInt(int64(point-1), 10))
	}
	return val.String()
}
