package cluster

import "time"

type NetworkConfig interface {
	Addrs() []string
	SmartRouting() bool
	ConnectionTimeout() time.Duration
}

type NetworkConfigBuilder interface {
	SetAddresses(addr ...string) NetworkConfigBuilder
	Config() (NetworkConfig, error)
}

type NetworkConfigProvider interface {
	Addresses() []string
	ConnectionTimeout() time.Duration
}
