package cluster

import (
	"fmt"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"time"
)

type NetworkConfigImpl struct {
	addresses         []string
	smartRouting      bool
	connectionTimeout time.Duration
}

func NewNetworkConfigImpl() *NetworkConfigImpl {
	defaultAddr := fmt.Sprintf("%s:%d", internal.DefaultHost, internal.DefaultPort)
	return &NetworkConfigImpl{
		addresses:         []string{defaultAddr},
		smartRouting:      true,
		connectionTimeout: 5 * time.Second,
	}
}

func (n NetworkConfigImpl) Addrs() []string {
	return n.addresses
}

func (n NetworkConfigImpl) ConnectionTimeout() time.Duration {
	return n.connectionTimeout
}

func (n NetworkConfigImpl) SmartRouting() bool {
	return n.smartRouting
}

type NetworkConfigBuilderImpl struct {
	networkConfig *NetworkConfigImpl
	err           error
}

func NewNetworkConfigBuilderImpl() *NetworkConfigBuilderImpl {
	return &NetworkConfigBuilderImpl{
		networkConfig: NewNetworkConfigImpl(),
	}
}

func (n *NetworkConfigBuilderImpl) SetAddrs(addresses ...string) icluster.NetworkConfigBuilder {
	selfAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := checkAddress(addr); err != nil {
			n.err = err
			return n
		}
		selfAddresses[i] = addr
	}
	n.networkConfig.addresses = selfAddresses
	return n
}

func (n *NetworkConfigBuilderImpl) SetSmartRouting(enable bool) icluster.NetworkConfigBuilder {
	n.networkConfig.smartRouting = enable
	return n
}

func (n *NetworkConfigBuilderImpl) SetConnectionTimeout(timeout time.Duration) icluster.NetworkConfigBuilder {
	n.networkConfig.connectionTimeout = timeout
	return n
}

func (n NetworkConfigBuilderImpl) Config() (icluster.NetworkConfig, error) {
	if n.err != nil {
		return n.networkConfig, n.err
	}
	return n.networkConfig, nil
}

func checkAddress(addr string) error {
	return nil
}
