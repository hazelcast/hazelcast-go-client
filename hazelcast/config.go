package hazelcast

import (
	"fmt"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

type ConfigProvider interface {
	Config() (*Config, error)
}

// Config contains configuration for a client.
// Although it is possible to set the values of the configuration directly,
// prefer to use the ConfigBuilder, since ConfigBuilder correctly sets the defaults.
type Config struct {
	ClientName          string
	ClusterConfig       cluster.Config
	SerializationConfig serialization.Config
	LoggerConfig        logger.Config
}

func (c Config) clone() Config {
	return Config{
		ClientName:          c.ClientName,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
		LoggerConfig:        c.LoggerConfig.Clone(),
	}
}

// ConfigBuilder is used to build configuration for a Hazelcast client.
type ConfigBuilder struct {
	config                     *Config
	clusterConfigBuilder       *ClusterConfigBuilder
	serializationConfigBuilder *SerializationConfigBuilder
	loggerConfigBuilder        *LoggerConfigBuilder
}

// NewConfigBuilder creates a new ConfigBuilder.
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config:                     &Config{},
		clusterConfigBuilder:       newClusterConfigBuilder(),
		serializationConfigBuilder: newSerializationConfigBuilder(),
		loggerConfigBuilder:        newLoggerConfigBuilder(),
	}
}

// SetClientName sets the client name
func (c *ConfigBuilder) SetClientName(name string) *ConfigBuilder {
	c.config.ClientName = name
	return c
}

// Cluster returns the cluster configuration builder.
func (c *ConfigBuilder) Cluster() *ClusterConfigBuilder {
	if c.clusterConfigBuilder == nil {
		c.clusterConfigBuilder = &ClusterConfigBuilder{}
	}
	return c.clusterConfigBuilder
}

// Serialization returns the serialization configuration builder.
func (c *ConfigBuilder) Serialization() *SerializationConfigBuilder {
	if c.serializationConfigBuilder == nil {
		c.serializationConfigBuilder = newSerializationConfigBuilder()
	}
	return c.serializationConfigBuilder
}

func (c *ConfigBuilder) Logger() *LoggerConfigBuilder {
	if c.loggerConfigBuilder == nil {
		c.loggerConfigBuilder = &LoggerConfigBuilder{}
	}
	return c.loggerConfigBuilder
}

// Config completes building the configuration and returns it.
func (c ConfigBuilder) Config() (*Config, error) {
	if c.clusterConfigBuilder != nil {
		if networkConfig, err := c.clusterConfigBuilder.buildConfig(); err != nil {
			return nil, err
		} else {
			c.config.ClusterConfig = *networkConfig
		}
	}
	if c.serializationConfigBuilder != nil {
		if serializationConfig, err := c.serializationConfigBuilder.buildConfig(); err != nil {
			return nil, err
		} else {
			c.config.SerializationConfig = *serializationConfig
		}
	}
	if c.loggerConfigBuilder != nil {
		if loggerConfig, err := c.loggerConfigBuilder.buildConfig(); err != nil {
			return nil, err
		} else {
			c.config.LoggerConfig = *loggerConfig
		}
	}
	return c.config, nil
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

// SetName sets the cluster name,
// The name is sent as part of the the client authentication message and may be verified on the member.
// The default cluster name is dev.
func (b *ClusterConfigBuilder) SetName(name string) *ClusterConfigBuilder {
	b.config.Name = name
	return b
}

// SetMembers sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
// By default 127.0.0.1:5701 is set as the member address.
func (b *ClusterConfigBuilder) SetMembers(addrs ...string) *ClusterConfigBuilder {
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

// SetSmartRouting enables or disables smart mode for the client.
// Smart clients send key based operations to owner of the keys.
// Unisocket clients send all operations to a single node.
// Smart routing is enabled by default.
func (b *ClusterConfigBuilder) SetSmartRouting(enable bool) *ClusterConfigBuilder {
	b.config.SmartRouting = enable
	return b
}

// SetConnectionTimeout is socket timeout value in seconds for client to connect member nodes.
// The default connection timeout is 5 seconds.
func (b *ClusterConfigBuilder) SetConnectionTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.ConnectionTimeout = timeout
	return b
}

// SetHeartbeatInterval sets time interval between the heartbeats sent by the client to the member nodes in seconds.
// The client sends heartbeats to the cluster in order to avoid stale connections.
// The default heartbeat interval is 5 seconds.
func (b *ClusterConfigBuilder) SetHeartbeatInterval(interval time.Duration) *ClusterConfigBuilder {
	b.config.HeartbeatInterval = interval
	return b
}

// SetHeartbeatTimeout sets the hearbeat timeout.
// If no request is sent to the cluster including heartbeats before timeout, the connection is closed.
func (b *ClusterConfigBuilder) SetHeartbeatTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.HeartbeatTimeout = timeout
	return b
}

// SetInvocationTimeout sets the invocation timeout
// When an invocation gets an exception because
// * Member throws an exception.
// * Connection between the client and member is closed.
// * Client’s heartbeat requests are timed out.
// Time passed since invocation started is compared with this property.
// If the time is already passed, then the exception is delegated to the user.
// If not, the invocation is retried.
// Note that, if invocation gets no exception and it is a long running one, then it will not get any exception, no matter how small this timeout is set.
// The default invocation timeout is 120 seconds.
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
			BigEndian:                           true,
			IdentifiedDataSerializableFactories: map[int32]serialization.IdentifiedDataSerializableFactory{},
			PortableFactories:                   map[int32]serialization.PortableFactory{},
			CustomSerializers:                   map[reflect.Type]serialization.Serializer{},
		},
	}
}

// SetBigEndian sets byte order to big endian.
// If set to false, little endian byte order is used.
// Default byte order is big endian.
func (b *SerializationConfigBuilder) SetBigEndian(enabled bool) *SerializationConfigBuilder {
	b.config.BigEndian = enabled
	return b
}

// SetGlobalSerializer sets the global serializer
func (b *SerializationConfigBuilder) SetGlobalSerializer(serializer serialization.Serializer) *SerializationConfigBuilder {
	if serializer.ID() <= 0 {
		panic("serializerID must be positive")
	}
	b.config.GlobalSerializer = serializer
	return b
}

// AddIdentifiedDataSerializableFactory adds an identified data serializable factory.
func (b *SerializationConfigBuilder) AddIdentifiedDataSerializableFactory(factory serialization.IdentifiedDataSerializableFactory) *SerializationConfigBuilder {
	b.config.IdentifiedDataSerializableFactories[factory.FactoryID()] = factory
	return b
}

// AddPortableFactory adds a portable factory.
func (b *SerializationConfigBuilder) AddPortableFactory(factory serialization.PortableFactory) *SerializationConfigBuilder {
	b.config.PortableFactories[factory.FactoryID()] = factory
	return b
}

// AddCustomSerializer adds a customer serializer for the given type.
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

type LoggerConfigBuilder struct {
	config *logger.Config
	err    error
}

func newLoggerConfigBuilder() *LoggerConfigBuilder {
	return &LoggerConfigBuilder{config: &logger.Config{
		Level: logger.InfoLevel,
	}}
}

func (b *LoggerConfigBuilder) SetLevel(level logger.Level) *LoggerConfigBuilder {
	b.config.Level = level
	return b
}

func (b *LoggerConfigBuilder) buildConfig() (*logger.Config, error) {
	return b.config, nil
}
