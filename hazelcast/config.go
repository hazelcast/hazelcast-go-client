package hazelcast

import (
	"fmt"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

type Config struct {
	ClientName          string
	Properties          map[string]string
	ClusterConfig       cluster.Config
	SerializationConfig serialization.Config
}

func (c Config) Clone() Config {
	properties := map[string]string{}
	for k, v := range c.Properties {
		properties[k] = v
	}
	return Config{
		ClientName:          c.ClientName,
		Properties:          properties,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
	}
}

type ConfigBuilder struct {
	config                     *Config
	clusterConfigBuilder       *ClusterConfigBuilder
	serializationConfigBuilder *SerializationConfigBuilder
}

func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: &Config{
			Properties: map[string]string{},
		},
		clusterConfigBuilder:       newClusterConfigBuilder(),
		serializationConfigBuilder: newSerializationConfigBuilder(),
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

func (c *ConfigBuilder) Cluster() *ClusterConfigBuilder {
	if c.clusterConfigBuilder == nil {
		c.clusterConfigBuilder = &ClusterConfigBuilder{}
	}
	return c.clusterConfigBuilder
}

func (c *ConfigBuilder) Serialization() *SerializationConfigBuilder {
	if c.serializationConfigBuilder == nil {
		c.serializationConfigBuilder = newSerializationConfigBuilder()
	}
	return c.serializationConfigBuilder
}

func (c ConfigBuilder) Config() (Config, error) {
	if c.clusterConfigBuilder != nil {
		if networkConfig, err := c.clusterConfigBuilder.buildConfig(); err != nil {
			return Config{}, err
		} else {
			c.config.ClusterConfig = *networkConfig
		}
	}
	if c.serializationConfigBuilder != nil {
		if serializationConfig, err := c.serializationConfigBuilder.buildConfig(); err != nil {
			return Config{}, err
		} else {
			c.config.SerializationConfig = *serializationConfig
		}
	}
	return *c.config, nil
}

func newClusterConfig() *cluster.Config {
	defaultAddr := fmt.Sprintf("%s:%d", cluster.DefaultHost, cluster.DefaultPort)
	return &cluster.Config{
		Name:              "dev",
		Addrs:             []string{defaultAddr},
		SmartRouting:      true,
		ConnectionTimeout: 5 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout:  60 * time.Second,
		InvocationTimeout: 120 * time.Second,
	}
}

type ClusterConfigBuilder struct {
	config *cluster.Config
	err    error
}

func newClusterConfigBuilder() *ClusterConfigBuilder {
	return &ClusterConfigBuilder{
		config: newClusterConfig(),
	}
}

func (b *ClusterConfigBuilder) SetName(name string) *ClusterConfigBuilder {
	b.config.Name = name
	return b
}

func (b *ClusterConfigBuilder) SetAddrs(addrs ...string) *ClusterConfigBuilder {
	selfAddresses := make([]string, len(addrs))
	for i, addr := range addrs {
		if err := checkAddress(addr); err != nil {
			b.err = err
			return b
		}
		selfAddresses[i] = addr
	}
	b.config.Addrs = selfAddresses
	return b
}

func (b *ClusterConfigBuilder) SetSmartRouting(enable bool) *ClusterConfigBuilder {
	b.config.SmartRouting = enable
	return b
}

func (b *ClusterConfigBuilder) SetConnectionTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.ConnectionTimeout = timeout
	return b
}

func (b *ClusterConfigBuilder) SetHeartbeatInterval(interval time.Duration) *ClusterConfigBuilder {
	b.config.HeartbeatInterval = interval
	return b
}

func (b *ClusterConfigBuilder) SetHeartbeatTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.HeartbeatTimeout = timeout
	return b
}

func (b *ClusterConfigBuilder) SetInvocationTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.InvocationTimeout = timeout
	return b
}

func (b *ClusterConfigBuilder) buildConfig() (*cluster.Config, error) {
	if b.err != nil {
		return b.config, b.err
	}
	return b.config, nil
}

func checkAddress(addr string) error {
	return nil
}

type SerializationConfigBuilder struct {
	config *serialization.Config
	err    error
}

func newSerializationConfigBuilder() *SerializationConfigBuilder {
	return &SerializationConfigBuilder{
		config: &serialization.Config{
			IdentifiedDataSerializableFactories: map[int32]serialization.IdentifiedDataSerializableFactory{},
			PortableFactories:                   map[int32]serialization.PortableFactory{},
			CustomSerializers:                   map[reflect.Type]serialization.Serializer{},
		},
	}
}

func (b *SerializationConfigBuilder) SetLittleEndian(enabled bool) *SerializationConfigBuilder {
	b.config.LittleEndian = enabled
	return b
}

func (b *SerializationConfigBuilder) SetGlobalSerializer(serializer serialization.Serializer) *SerializationConfigBuilder {
	if serializer.ID() <= 0 {
		panic("serializerID must be positive")
	}
	b.config.GlobalSerializer = serializer
	return b
}

func (b *SerializationConfigBuilder) AddIdentifiedDataSerializableFactory(factoryID int32, factory serialization.IdentifiedDataSerializableFactory) *SerializationConfigBuilder {
	b.config.IdentifiedDataSerializableFactories[factoryID] = factory
	return b
}

func (b *SerializationConfigBuilder) AddPortableFactory(factoryID int32, factory serialization.PortableFactory) *SerializationConfigBuilder {
	b.config.PortableFactories[factoryID] = factory
	return b
}

func (b *SerializationConfigBuilder) AddCustomSerializer(t reflect.Type, serializer serialization.Serializer) *SerializationConfigBuilder {
	if serializer.ID() <= 0 {
		panic("serializerID must be positive")
	}
	b.config.CustomSerializers[t] = serializer
	return b
}

func (b *SerializationConfigBuilder) AddClassDefinition(definition serialization.ClassDefinition) *SerializationConfigBuilder {
	b.config.ClassDefinitions = append(b.config.ClassDefinitions, definition)
	return b
}

func (b *SerializationConfigBuilder) buildConfig() (*serialization.Config, error) {
	if b.err != nil {
		return b.config, b.err
	}
	return b.config, nil
}
