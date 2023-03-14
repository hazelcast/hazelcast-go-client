/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package types_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestDecimal(t *testing.T) {
	testCases := []struct {
		f    func(t *testing.T)
		name string
	}{
		{name: "TestDecimalToString", f: decimalStringTest},
		{name: "TestDecimalToStringOverflow", f: decimalStringOverflowTest},
		{name: "TestDecimalToFloat64", f: decimalFloat64Test},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func decimalStringTest(t *testing.T) {
	testCases := []struct {
		dec types.Decimal
		str string
	}{
		{dec: types.NewDecimal(big.NewInt(123), 0), str: "123"},
		{dec: types.NewDecimal(big.NewInt(-123), 0), str: "-123"},
		{dec: types.NewDecimal(big.NewInt(-123), 2), str: "-1.23"},
		{dec: types.NewDecimal(big.NewInt(-123), 5), str: "-0.00123"},
		{dec: types.NewDecimal(big.NewInt(123), -1), str: "1.23E+3"},
		{dec: types.NewDecimal(big.NewInt(123), -3), str: "1.23E+5"},
		{dec: types.NewDecimal(big.NewInt(123), 1), str: "12.3"},
		{dec: types.NewDecimal(big.NewInt(123), 5), str: "0.00123"},
		{dec: types.NewDecimal(big.NewInt(123), 10), str: "1.23E-8"},
		{dec: types.NewDecimal(big.NewInt(-123), 12), str: "-1.23E-10"},
		{dec: types.NewDecimal(big.NewInt(1), 10), str: "1E-10"},
		{dec: types.NewDecimal(big.NewInt(1), -10), str: "1E+10"},
		{dec: types.NewDecimal(big.NewInt(0), -1), str: "0E+1"},
		{dec: types.NewDecimal(big.NewInt(0), 1), str: "0.0"},
		{dec: types.NewDecimal(big.NewInt(0), 0), str: "0"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.dec.String())
	}
}

func decimalStringOverflowTest(t *testing.T) {
	skip.If(t, "arch ~ 32bit")
	max32 := math.MaxInt32
	min32 := math.MinInt32
	max64 := int64(math.MaxInt64)
	min64 := int64(math.MinInt64)
	overflowMax := max32 + 1
	overflowMin := min32 - 1
	testCases := []struct {
		dec types.Decimal
		str string
	}{
		{dec: types.NewDecimal(big.NewInt(123_456_789), min32), str: "1.23456789E+2147483656"},
		{dec: types.NewDecimal(big.NewInt(123_456_789), max32), str: "1.23456789E-2147483639"},
		{dec: types.NewDecimal(big.NewInt(int64(max32)), min32), str: "2.147483647E+2147483657"},
		{dec: types.NewDecimal(big.NewInt(int64(max32)), max32), str: "2.147483647E-2147483638"},
		{dec: types.NewDecimal(big.NewInt(int64(min32)), min32), str: "-2.147483648E+2147483657"},
		{dec: types.NewDecimal(big.NewInt(int64(min32)), max32), str: "-2.147483648E-2147483638"},
		{dec: types.NewDecimal(big.NewInt(111_111_111), overflowMax), str: "1.11111111E+2147483656"},
		{dec: types.NewDecimal(big.NewInt(111_111_111), overflowMin), str: "1.11111111E-2147483639"},
		{dec: types.NewDecimal(big.NewInt(int64(max32)), overflowMin), str: "2.147483647E-2147483638"},
		{dec: types.NewDecimal(big.NewInt(int64(max32)), overflowMax), str: "2.147483647E+2147483657"},
		{dec: types.NewDecimal(big.NewInt(int64(min32)), overflowMin), str: "-2.147483648E-2147483638"},
		{dec: types.NewDecimal(big.NewInt(int64(min32)), overflowMax), str: "-2.147483648E+2147483657"},
		{dec: types.NewDecimal(big.NewInt(max64), overflowMin), str: "9.223372036854775807E-2147483629"},
		{dec: types.NewDecimal(big.NewInt(max64), overflowMax), str: "9.223372036854775807E+2147483666"},
		{dec: types.NewDecimal(big.NewInt(min64), overflowMin), str: "-9.223372036854775808E-2147483629"},
		{dec: types.NewDecimal(big.NewInt(min64), overflowMax), str: "-9.223372036854775808E+2147483666"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.dec.String())
	}
}

func decimalFloat64Test(t *testing.T) {
	testCases := []struct {
		dec types.Decimal
		flt float64
	}{
		{dec: types.NewDecimal(big.NewInt(123), 0), flt: 123},
		{dec: types.NewDecimal(big.NewInt(-123), 0), flt: -123},
		{dec: types.NewDecimal(big.NewInt(-123), 2), flt: -1.23},
		{dec: types.NewDecimal(big.NewInt(-123), 5), flt: -0.00123},
		{dec: types.NewDecimal(big.NewInt(123), -1), flt: 1.23e+3},
		{dec: types.NewDecimal(big.NewInt(123), -3), flt: 1.23e+5},
		{dec: types.NewDecimal(big.NewInt(123), 1), flt: 12.3},
		{dec: types.NewDecimal(big.NewInt(123), 5), flt: 0.00123},
		{dec: types.NewDecimal(big.NewInt(123), 10), flt: 1.23e-8},
		{dec: types.NewDecimal(big.NewInt(-123), 12), flt: -1.23e-10},
		{dec: types.NewDecimal(big.NewInt(1), 10), flt: 1e-10},
		{dec: types.NewDecimal(big.NewInt(1), -10), flt: 1e+10},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.flt, tc.dec.Float64())
	}
}
