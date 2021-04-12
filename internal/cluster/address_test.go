package cluster_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
)

type addrCase struct {
	input string
	host  string
	port  int
	err   error
}

func TestAddressImplParse(t *testing.T) {
	cases := []addrCase{
		{input: "", host: "localhost", port: 5701},
		{input: "localhost", host: "localhost", port: 5701},
		{input: ":4566", err: errors.New("error parsing address: address with no host")},
		{input: "foo.com:2223", host: "foo.com", port: 2223},
		// TODO: ipv6
	}
	for i, addrCase := range cases {
		if err := assertAddress(addrCase); err != nil {
			t.Error(fmt.Errorf("TestAddressImplParse test %d error: %w", i+1, err))
		}
	}
}

func assertAddress(addrCase addrCase) error {
	addr, err := cluster.ParseAddress(addrCase.input)
	if addrCase.err != nil {
		if !reflect.DeepEqual(addrCase.err, err) {
			return fmt.Errorf("target err: %v != %v", addrCase.err, err)
		}
		return nil
	} else if err != nil {
		return err
	}
	if addrCase.host != addr.Host() {
		return fmt.Errorf("target host: %v != %v", addrCase.host, addr.Host())
	}
	if addrCase.port != addr.Port() {
		return fmt.Errorf("target port: %v != %v", addrCase.port, addr.Port())
	}
	return nil
}
