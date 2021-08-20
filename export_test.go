package hazelcast

// Exports non-exported types and methods to hazelcast_test package.

const (
	DefaultFlakeIDPrefetchCount  = defaultFlakeIDPrefetchCount
	DefaultFlakeIDPrefetchExpiry = defaultFlakeIDPrefetchExpiry
)

func (c *Config) GetFlakeIDGeneratorConfig(name string) FlakeIDGeneratorConfig {
	return c.getFlakeIDGeneratorConfig(name)
}
