package cluster

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
)

type AddressProvider interface {
	Addresses() []pubcluster.Address
}

type DefaultAddressProvider struct {
	addresses []pubcluster.Address
}

func ParseAddress(addr string) (*pubcluster.AddressImpl, error) {
	// first check whether addr contains the port
	if index := strings.Index(addr, ":"); index == 0 {
		return nil, errors.New("error parsing address: address with no host")
	} else if index < 0 {
		if addr == "" {
			// default address
			return pubcluster.NewAddressWithHostPort(pubcluster.DefaultHost, pubcluster.DefaultPort), nil
		}
		// this is probably a string with only the host
		return pubcluster.NewAddressWithHostPort(addr, pubcluster.DefaultPort), nil
	}
	if host, portStr, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("error parsing address: %w", err)
	} else {
		if port, err := strconv.Atoi(portStr); err != nil {
			return nil, fmt.Errorf("error parsing address: %w", err)
		} else {
			return pubcluster.NewAddressWithHostPort(host, port), nil
		}
	}
}

func NewDefaultAddressProvider(networkConfig *pubcluster.Config) *DefaultAddressProvider {
	var err error
	addresses := make([]pubcluster.Address, len(networkConfig.Addrs))
	for i, addr := range networkConfig.Addrs {
		if addresses[i], err = ParseAddress(addr); err != nil {
			panic(err)
		}
	}
	return &DefaultAddressProvider{addresses: addresses}
}

func (p DefaultAddressProvider) Addresses() []pubcluster.Address {
	return p.addresses
}
