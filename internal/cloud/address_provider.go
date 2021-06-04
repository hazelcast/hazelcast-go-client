package cloud

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
)

type AddressProvider struct {
	addrs []pubcluster.Address
}

func NewAddressProvider(addrs []Address) (*AddressProvider, error) {
	if pubAddrs, err := translateAddrs(addrs); err != nil {
		return nil, err
	} else {
		return &AddressProvider{addrs: pubAddrs}, nil
	}
}

func (a AddressProvider) Addresses() []pubcluster.Address {
	return a.addrs
}

func translateAddrs(addrs []Address) ([]pubcluster.Address, error) {
	pubAddrs := make([]pubcluster.Address, len(addrs))
	for i, addr := range addrs {
		if pubAddr, err := cluster.ParseAddress(addr.Public); err != nil {
			return nil, err
		} else {
			pubAddrs[i] = pubAddr
		}
	}
	return pubAddrs, nil
}
