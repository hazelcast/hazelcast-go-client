package client

type Config struct {
	ClientName  string
	ClusterName string
	Network     NetworkConfig
}

type NetworkConfig struct {
	Addresses    []string
	SmartRouting bool
}

type ConfigProvider interface {
	Config() (Config, error)
}

type ConfigBuilder interface {
	SetClientName(name string) ConfigBuilder
	SetClusterName(name string) ConfigBuilder
	Network() NetworkConfigBuilder
	Config() (Config, error)
}

type NetworkConfigBuilder interface {
	SetAddresses(addr ...string) NetworkConfigBuilder
	config() (NetworkConfig, error)
}

type NetworkConfigProvider interface {
	Addresses() []string
}
