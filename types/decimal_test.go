package types_test

import (
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

func TestDecimal(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "TestDecimalToString", f: decimalStringTest},
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
	}
	for _, tc := range testCases {
		toStr := tc.dec.String()
		require.Equal(t, tc.str, toStr)
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
		toFlt := tc.dec.Float64()
		require.Equal(t, tc.flt, toFlt)
	}
}
