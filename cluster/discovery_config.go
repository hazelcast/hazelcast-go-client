package cluster

type DiscoveryConfig struct {
	UsePublicIP bool
}

func NewDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{}
}

func (c DiscoveryConfig) Clone() DiscoveryConfig {
	return c
}

func (c DiscoveryConfig) Validate() error {
	return nil
}
