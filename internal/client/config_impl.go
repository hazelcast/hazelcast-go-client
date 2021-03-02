package client

type ConfigBuilderImpl struct {
	config               Config
	networkConfigBuilder *NetworkConfigBuilderImpl
}

func NewConfigBuilderImpl() *ConfigBuilderImpl {
	return &ConfigBuilderImpl{}
}

func (c *ConfigBuilderImpl) SetClientName(name string) ConfigBuilder {
	c.config.ClientName = name
	return c
}

func (c *ConfigBuilderImpl) SetClusterName(name string) ConfigBuilder {
	c.config.ClusterName = name
	return c
}

func (c *ConfigBuilderImpl) Network() NetworkConfigBuilder {
	if c.networkConfigBuilder == nil {
		c.networkConfigBuilder = &NetworkConfigBuilderImpl{}
	}
	return c.networkConfigBuilder
}

func (c ConfigBuilderImpl) Config() (Config, error) {
	if c.networkConfigBuilder != nil {
		if networkConfig, err := c.networkConfigBuilder.config(); err != nil {
			return Config{}, err
		} else {
			c.config.Network = networkConfig
		}
	}
	return c.config, nil
}

type NetworkConfigBuilderImpl struct {
	networkConfig NetworkConfig
	err           error
}

func (n *NetworkConfigBuilderImpl) SetAddresses(addresses ...string) NetworkConfigBuilder {
	selfAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := checkAddress(addr); err != nil {
			n.err = err
			return n
		}
		selfAddresses[i] = addr
	}
	n.networkConfig.Addresses = selfAddresses
	return n
}

func (n NetworkConfigBuilderImpl) config() (NetworkConfig, error) {
	if n.err != nil {
		return n.networkConfig, n.err
	}
	return n.networkConfig, nil
}

type NetworkConfigImpl struct {
	addresses []string
}

func (n NetworkConfigImpl) Addresses() []string {
	return n.addresses
}

func checkAddress(addr string) error {
	return nil
}
