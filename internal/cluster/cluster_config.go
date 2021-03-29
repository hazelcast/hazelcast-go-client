package cluster

import (
	"fmt"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"time"
)

type ClusterConfigImpl struct {
	name              string
	addresses         []string
	smartRouting      bool
	connectionTimeout time.Duration
}

func NewClusterConfigImpl() *ClusterConfigImpl {
	defaultAddr := fmt.Sprintf("%s:%d", internal.DefaultHost, internal.DefaultPort)
	return &ClusterConfigImpl{
		name:              "dev",
		addresses:         []string{defaultAddr},
		smartRouting:      true,
		connectionTimeout: 5 * time.Second,
	}
}

func (c ClusterConfigImpl) Name() string {
	return c.name
}

func (c ClusterConfigImpl) Addrs() []string {
	return c.addresses
}

func (c ClusterConfigImpl) ConnectionTimeout() time.Duration {
	return c.connectionTimeout
}

func (c ClusterConfigImpl) SmartRouting() bool {
	return c.smartRouting
}

type ClusterConfigBuilderImpl struct {
	clusterConfig *ClusterConfigImpl
	err           error
}

func NewNetworkConfigBuilderImpl() *ClusterConfigBuilderImpl {
	return &ClusterConfigBuilderImpl{
		clusterConfig: NewClusterConfigImpl(),
	}
}

func (n *ClusterConfigBuilderImpl) SetName(name string) icluster.ClusterConfigBuilder {
	n.clusterConfig.name = name
	return n
}

func (n *ClusterConfigBuilderImpl) SetAddrs(addresses ...string) icluster.ClusterConfigBuilder {
	selfAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := checkAddress(addr); err != nil {
			n.err = err
			return n
		}
		selfAddresses[i] = addr
	}
	n.clusterConfig.addresses = selfAddresses
	return n
}

func (n *ClusterConfigBuilderImpl) SetSmartRouting(enable bool) icluster.ClusterConfigBuilder {
	n.clusterConfig.smartRouting = enable
	return n
}

func (n *ClusterConfigBuilderImpl) SetConnectionTimeout(timeout time.Duration) icluster.ClusterConfigBuilder {
	n.clusterConfig.connectionTimeout = timeout
	return n
}

func (n ClusterConfigBuilderImpl) Config() (icluster.ClusterConfig, error) {
	if n.err != nil {
		return n.clusterConfig, n.err
	}
	return n.clusterConfig, nil
}

func checkAddress(addr string) error {
	return nil
}
