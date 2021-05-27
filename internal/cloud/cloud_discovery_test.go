package cloud

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractAddresses(t *testing.T) {
	s := `[{"private-address":"100.115.50.221","public-address":"35.177.212.248:31984"},{"private-address":"100.109.198.133","public-address":"3.8.123.82:31984"}]`
	r := []interface{}{}
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatal(err)
	}
	addrs := extractAddresses(r)
	target := []Address{
		NewAddress("35.177.212.248:31984", "100.115.50.221"),
		NewAddress("3.8.123.82:31984", "100.109.198.133"),
	}
	assert.Equal(t, target, addrs)
}

func TestAugmentPrivateAddr(t *testing.T) {
	testCases := []struct {
		Pr string
		Pu string
		T  string
	}{
		{Pr: "100.109.198.133", Pu: "3.8.123.82:31984", T: "100.109.198.133:31984"},
		{Pr: "100.109.198.133:5555", Pu: "3.8.123.82:31984", T: "100.109.198.133:5555"},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert.Equal(t, tc.T, augmentPrivateAddr(tc.Pr, tc.Pu))
		})
	}
}
