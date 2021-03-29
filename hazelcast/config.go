package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
)

type Config struct {
	ClientName    string
	Properties    map[string]string
	ClusterConfig cluster.ClusterConfig
}

type ConfigProvider interface {
	Config() (Config, error)
}

type ConfigBuilder struct {
	config               Config
	clusterConfigBuilder *icluster.ClusterConfigBuilderImpl
}

func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: Config{
			Properties: map[string]string{},
		},
		clusterConfigBuilder: icluster.NewClusterConfigBuilderImpl(),
	}
}

func (c *ConfigBuilder) SetClientName(name string) *ConfigBuilder {
	c.config.ClientName = name
	return c
}

func (c *ConfigBuilder) SetProperty(key, value string) *ConfigBuilder {
	c.config.Properties[key] = value
	return c
}

func (c *ConfigBuilder) Cluster() cluster.ClusterConfigBuilder {
	if c.clusterConfigBuilder == nil {
		c.clusterConfigBuilder = &icluster.ClusterConfigBuilderImpl{}
	}
	return c.clusterConfigBuilder
}

func (c ConfigBuilder) Config() (Config, error) {
	if c.clusterConfigBuilder != nil {
		if networkConfig, err := c.clusterConfigBuilder.Config(); err != nil {
			return Config{}, err
		} else {
			c.config.ClusterConfig = networkConfig
		}
	}
	return c.config, nil
}
