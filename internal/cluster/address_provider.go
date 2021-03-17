package cluster

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
)

type AddressProvider interface {
	Addresses() []pubcluster.Address
}

type DefaultAddressProvider struct {
	addresses []pubcluster.Address
}

func NewDefaultAddressProvider(networkConfig pubcluster.NetworkConfig) *DefaultAddressProvider {
	var err error
	addresses := make([]pubcluster.Address, len(networkConfig.Addrs()))
	for i, addr := range networkConfig.Addrs() {
		if addresses[i], err = pubcluster.ParseAddress(addr); err != nil {
			panic(err)
		}
	}
	return &DefaultAddressProvider{addresses: addresses}
}

func (p DefaultAddressProvider) Addresses() []pubcluster.Address {
	return p.addresses
}
