package cluster

import "github.com/hazelcast/hazelcast-go-client/v4/internal/core"

type AddressProvider interface {
	Addresses() []*AddressImpl
}

type DefaultAddressProvider struct {
	addresses []*AddressImpl
}

func NewDefaultAddressProvider(networkConfig NetworkConfig) *DefaultAddressProvider {
	var err error
	addresses := make([]*AddressImpl, len(networkConfig.Addrs()))
	for i, addr := range networkConfig.Addrs() {
		if addresses[i], err = core.ParseAddress(addr); err != nil {
			panic(err)
		}
	}
	return &DefaultAddressProvider{addresses: addresses}
}

func (p DefaultAddressProvider) Addresses() []*AddressImpl {
	return p.addresses
}
