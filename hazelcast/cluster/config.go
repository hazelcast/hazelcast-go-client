package cluster

import "time"

type NetworkConfig interface {
	Addrs() []string
	SmartRouting() bool
	ConnectionTimeout() time.Duration
}

type NetworkConfigBuilder interface {
	SetAddrs(addr ...string) NetworkConfigBuilder
	SetSmartRouting(enable bool) NetworkConfigBuilder
	Config() (NetworkConfig, error)
}

type NetworkConfigProvider interface {
	Addrs() []string
	ConnectionTimeout() time.Duration
}
