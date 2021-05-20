/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Config contains configuration for a client.
// Prefer to create the configuration using the NewConfig function.
type Config struct {
	ClientName          string
	ClusterConfig       cluster.Config
	SerializationConfig serialization.Config
	LoggerConfig        logger.Config
	lifecycleListeners  map[types.UUID]LifecycleStateChangeHandler
	membershipListeners map[types.UUID]cluster.MembershipStateChangeHandler
}

func NewConfig() Config {
	config := Config{
		ClusterConfig:       cluster.NewConfig(),
		SerializationConfig: serialization.NewConfig(),
		LoggerConfig:        logger.NewConfig(),
		lifecycleListeners:  map[types.UUID]LifecycleStateChangeHandler{},
		membershipListeners: map[types.UUID]cluster.MembershipStateChangeHandler{},
	}
	return config
}

// AddLifecycleListener adds a lifecycle listener.
// The listener is attached to the client before the client starts, so all lifecycle events can be received.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Config) AddLifecycleListener(handler LifecycleStateChangeHandler) types.UUID {
	if c.lifecycleListeners == nil {
		c.lifecycleListeners = map[types.UUID]LifecycleStateChangeHandler{}
	}
	id := types.NewUUID()
	c.lifecycleListeners[id] = handler
	return id
}

// AddMembershipListener adds a membership listeener.
// The listener is attached to the client before the client starts, so all membership events can be received.
// Use the returned subscription ID to remove the listener.
func (c *Config) AddMembershipListener(handler cluster.MembershipStateChangeHandler) types.UUID {
	if c.membershipListeners == nil {
		c.membershipListeners = map[types.UUID]cluster.MembershipStateChangeHandler{}
	}
	id := types.NewUUID()
	c.membershipListeners[id] = handler
	return id
}

func (c Config) Clone() Config {
	return Config{
		ClientName:          c.ClientName,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
		LoggerConfig:        c.LoggerConfig.Clone(),
		// both lifecycleListeners and membershipListeners are not used verbatim in client creator
		// so no need to copy them
		lifecycleListeners:  c.lifecycleListeners,
		membershipListeners: c.membershipListeners,
	}
}

/*
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
		config: &Config{
			lifecycleListeners:  map[types.UUID]LifecycleStateChangeHandler{},
			membershipListeners: map[types.UUID]cluster.MembershipStateChangeHandler{},
		},
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
	return c.clusterConfigBuilder
}

// Serialization returns the serialization configuration builder.
func (c *ConfigBuilder) Serialization() *SerializationConfigBuilder {
	return c.serializationConfigBuilder
}

func (c *ConfigBuilder) Logger() *LoggerConfigBuilder {
	return c.loggerConfigBuilder
}

// Config completes building the configuration and returns it.
func (c ConfigBuilder) Config() (*Config, error) {
	if c.clusterConfigBuilder != nil {
		if clusterConfig, err := c.clusterConfigBuilder.buildConfig(); err != nil {
			return nil, err
		} else {
			c.config.ClusterConfig = *clusterConfig
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
	config                *cluster.Config
	securityConfigBuilder *ClusterSecurityConfigBuilder
	sslConfigBuilder      *ClusterSSLConfigBuilder
	err                   error
}

func newClusterConfigBuilder() *ClusterConfigBuilder {
	return &ClusterConfigBuilder{
		config:                newClusterConfig(),
		securityConfigBuilder: newClusterSecurityConfigBuilder(),
		sslConfigBuilder:      newClusterSSLConfigBuilder(),
	}
}

// SetName sets the cluster name,
// The name is sent as part of the the client authentication message and may be verified on the member.
// The default cluster name is dev.
func (b *ClusterConfigBuilder) SetName(name string) *ClusterConfigBuilder {
	b.config.Name = name
	return b
}

// AddAddrs sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
// By default localhost:5701 is set as the member address.
func (b *ClusterConfigBuilder) AddAddrs(addrs ...string) *ClusterConfigBuilder {
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

// SetConnectionTimeout is socket timeout value for client to connect member nodes.
// The default connection timeout is 5 seconds.
func (b *ClusterConfigBuilder) SetConnectionTimeout(timeout time.Duration) *ClusterConfigBuilder {
	b.config.ConnectionTimeout = timeout
	return b
}

// SetHeartbeatInterval sets time interval between the heartbeats sent by the client to the member nodes.
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

func (b *ClusterConfigBuilder) SetRedoOperation(enable bool) *ClusterConfigBuilder {
	b.config.RedoOperation = enable
	return b
}

func (b *ClusterConfigBuilder) Security() *ClusterSecurityConfigBuilder {
	return b.securityConfigBuilder
}

func (b *ClusterConfigBuilder) SSL() *ClusterSSLConfigBuilder {
	return b.sslConfigBuilder
}

func (b *ClusterConfigBuilder) buildConfig() (*cluster.Config, error) {
	if b.err != nil {
		return nil, b.err
	}
	b.config.SecurityConfig = *b.securityConfigBuilder.buildConfig()
	if sslConfig, err := b.sslConfigBuilder.buildConfig(); err != nil {
		return nil, err
	} else {
		b.config.SSLConfig = *sslConfig
	}
	return b.config, nil
}

func checkAddress(addr string) error {
	return nil
}

type ClusterSecurityConfigBuilder struct {
	config *cluster.SecurityConfig
}

func newClusterSecurityConfigBuilder() *ClusterSecurityConfigBuilder {
	return &ClusterSecurityConfigBuilder{config: &cluster.SecurityConfig{}}
}

func (b *ClusterSecurityConfigBuilder) SetCredentials(username string, password string) *ClusterSecurityConfigBuilder {
	b.config.Username = username
	b.config.Password = password
	return b
}

func (b *ClusterSecurityConfigBuilder) buildConfig() *cluster.SecurityConfig {
	return b.config
}

type ClusterSSLConfigBuilder struct {
	config *cluster.SSLConfig
	err    error
}

func newClusterSSLConfigBuilder() *ClusterSSLConfigBuilder {
	return &ClusterSSLConfigBuilder{config: &cluster.SSLConfig{TLSConfig: &tls.Config{}}}
}

func (b *ClusterSSLConfigBuilder) buildConfig() (*cluster.SSLConfig, error) {
	return b.config, b.err
}

func (b *ClusterSSLConfigBuilder) SetEnabled(enabled bool) *ClusterSSLConfigBuilder {
	b.config.Enabled = enabled
	return b
}

func (b *ClusterSSLConfigBuilder) ResetTLSConfig(tlsConfig *tls.Config) *ClusterSSLConfigBuilder {
	b.config.TLSConfig = tlsConfig.Clone()
	return b
}
*/
