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
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Decimal is a wrapper for Hazelcast Decimal.
type Decimal struct {
	unscaledValue *big.Int
	scale         int32
}

// NewDecimal creates and returns a Decimal value with the given big int and scale.
func NewDecimal(unscaledValue *big.Int, scale int) Decimal {
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
func (d Decimal) Scale() int {
	return int(d.scale)
}

// Float64 converts the decimal to a float64 and returns it.
// Note that this conversion can lose information about the precision of the decimal value.
func (d Decimal) Float64() float64 {
	return float64(d.UnscaledValue().Int64()) / math.Pow10(d.Scale())
}

// String returns the string representation of the decimal, using scientific notation if an exponent is needed.
func (d Decimal) String() string {
	// This implementation is ported from: java.math.BigDecimal#layoutChars
	bigStr := d.unscaledValue.String()
	if d.scale == 0 {
		return bigStr
	}
	buf := strings.Builder{}
	if bigStr[0] == '-' {
		bigStr = bigStr[1:]
		buf.WriteByte('-')
	}
	adjusted := len(bigStr) - int(d.scale) - 1
	if d.scale >= 0 && adjusted >= -6 {
		// plain number
		pad := int(d.scale) - len(bigStr)
		if pad >= 0 {
			// 0.xxx form
			buf.WriteString("0.")
			for pad > 0 {
				buf.WriteByte('0')
				pad--
			}
			buf.WriteString(bigStr)
		} else {
			// xx.xx form
			pad *= -1
			buf.WriteString(bigStr[0:pad] + "." + bigStr[pad:])
		}
	} else {
		// E-notation is needed
		buf.WriteByte(bigStr[0])
		if len(bigStr) > 1 {
			buf.WriteString("." + bigStr[1:])
		}
		buf.WriteByte('E')
		if adjusted > 0 {
			buf.WriteByte('+')
		}
		buf.WriteString(strconv.FormatInt(int64(adjusted), 10))
	}
	return buf.String()
}
