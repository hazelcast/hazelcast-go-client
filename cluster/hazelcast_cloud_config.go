package cluster

type HazelcastCloudConfig struct {
	Token   string
	Enabled bool
}

func NewHazelcastCloudConfig() HazelcastCloudConfig {
	return HazelcastCloudConfig{}
}

func (h HazelcastCloudConfig) Clone() HazelcastCloudConfig {
	return h
}

func (h HazelcastCloudConfig) Validate() error {
	return nil
}
