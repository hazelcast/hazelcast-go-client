package cloud

import (
	"context"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type AddressProvider struct {
	addrs []pubcluster.Address
}

func NewAddressProvider(config *pubcluster.Config, logger logger.Logger) (*AddressProvider, error) {
	dc := NewDiscoveryClient(&config.HazelcastCloudConfig, logger)
	if addrs, err := dc.DiscoverNodes(context.Background()); err != nil {
		return nil, err
	} else if pubAddrs, err := translateAddrs(addrs); err != nil {
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
