package types_test

import (
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

func TestDecimal_String(t *testing.T) {
	decimals := [8]types.Decimal{
		types.NewDecimal(big.NewInt(123), 0),
		types.NewDecimal(big.NewInt(-123), 0),
		types.NewDecimal(big.NewInt(123), -1),
		types.NewDecimal(big.NewInt(123), -3),
		types.NewDecimal(big.NewInt(123), 1),
		types.NewDecimal(big.NewInt(123), 5),
		types.NewDecimal(big.NewInt(123), 10),
		types.NewDecimal(big.NewInt(-123), 12),
	}
	targets := [8]string{
		"123",
		"-123",
		"1.23E+3",
		"1.23E+5",
		"12.3",
		"0.00123",
		"1.23E-8",
		"-1.23E-10",
	}
	for i := 0; i < len(decimals); i++ {
		str := decimals[i].String()
		require.Equal(t, targets[i], str)
	}
}

func TestDecimal_Float64(t *testing.T) {
	decimals := [8]types.Decimal{
		types.NewDecimal(big.NewInt(123), 0),
		types.NewDecimal(big.NewInt(-123), 0),
		types.NewDecimal(big.NewInt(123), -1),
		types.NewDecimal(big.NewInt(123), -3),
		types.NewDecimal(big.NewInt(123), 1),
		types.NewDecimal(big.NewInt(123), 5),
		types.NewDecimal(big.NewInt(123), 10),
		types.NewDecimal(big.NewInt(-123), 12),
	}
	targets := [8]float64{
		123,
		-123,
		1.23e+3,
		1.23e+5,
		12.3,
		0.00123,
		1.23e-8,
		-1.23e-10,
	}
	for i := 0; i < len(decimals); i++ {
		str := decimals[i].Float64()
		require.Equal(t, targets[i], str)
	}
}
