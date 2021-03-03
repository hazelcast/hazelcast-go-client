package cluster

type NetworkConfig struct {
	Addresses    []string
	SmartRouting bool
}

type NetworkConfigBuilder interface {
	SetAddresses(addr ...string) NetworkConfigBuilder
	Config() (NetworkConfig, error)
}

type NetworkConfigProvider interface {
	Addresses() []string
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

func (n NetworkConfigBuilderImpl) Config() (NetworkConfig, error) {
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
