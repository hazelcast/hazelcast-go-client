package cluster

import "time"

type ClusterConfig interface {
	Name() string
	Addrs() []string
	SmartRouting() bool
	ConnectionTimeout() time.Duration
}

type ClusterConfigBuilder interface {
	SetName(name string) ClusterConfigBuilder
	SetAddrs(addr ...string) ClusterConfigBuilder
	SetSmartRouting(enable bool) ClusterConfigBuilder
	Config() (ClusterConfig, error)
}

// TODO: remove
type ClusterConfigProvider interface {
	Addrs() []string
	ConnectionTimeout() time.Duration
}
