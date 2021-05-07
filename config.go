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
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
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
	lifecycleListeners  map[types.UUID]LifecycleStateChangeHandler
	membershipListeners map[types.UUID]cluster.MembershipStateChangeHandler
}

func (c Config) clone() Config {
	return Config{
		ClientName:          c.ClientName,
		ClusterConfig:       c.ClusterConfig.Clone(),
		SerializationConfig: c.SerializationConfig.Clone(),
		LoggerConfig:        c.LoggerConfig.Clone(),
		lifecycleListeners:  c.lifecycleListeners,
		membershipListeners: c.membershipListeners,
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

// AddLifecycleListener adds a lifecycle listener.
// The listener is attached to the client before the client starts, so all lifecycle events can be received.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *ConfigBuilder) AddLifecycleListener(handler LifecycleStateChangeHandler) types.UUID {
	id := types.NewUUID()
	c.config.lifecycleListeners[id] = handler
	return id
}

// AddMembershipListener adds a membership listeener.
// The listener is attached to the client before the client starts, so all membership events can be received.
// Use the returned subscription ID to remove the listener.
func (c *ConfigBuilder) AddMembershipListener(handler cluster.MembershipStateChangeHandler) types.UUID {
	id := types.NewUUID()
	c.config.membershipListeners[id] = handler
	return id
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

// SetAddrs sets the candidate address list that client will use to establish initial connection.
// Other members of the cluster will be discovered when the client starts.
// By default localhost:5701 is set as the member address.
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

// SetCAPath sets CA file path.
func (b *ClusterSSLConfigBuilder) SetCAPath(path string) *ClusterSSLConfigBuilder {
	// XXX: what happens if the path is loaded multiple times?
	// load CA cert
	if caCert, err := ioutil.ReadFile(path); err != nil {
		b.err = err
	} else {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			b.err = hzerror.NewHazelcastIOError("error while loading the CA file, make sure the path exits and "+
				"the format is pem", nil)
		} else {
			b.config.TLSConfig.RootCAs = caCertPool
		}
	}
	return b
}

// AddClientCertAndKeyPath adds client certificate path and client private key path to tls config.
// The files in the given paths must contain PEM encoded data.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (b *ClusterSSLConfigBuilder) AddClientCertAndKeyPath(clientCertPath string, clientPrivateKeyPath string) *ClusterSSLConfigBuilder {
	if cert, err := tls.LoadX509KeyPair(clientCertPath, clientPrivateKeyPath); err != nil {
		b.err = err
	} else {
		b.config.TLSConfig.Certificates = append(b.config.TLSConfig.Certificates, cert)
	}
	return b
}

// AddClientCertAndEncryptedKeyPath decrypts the keyfile with the given password and
// adds client certificate path and the decrypted client private key to tls config.
// The files in the given paths must contain PEM encoded data.
// The key file should have a DEK-info header otherwise an error will be returned.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (b *ClusterSSLConfigBuilder) AddClientCertAndEncryptedKeyPath(certPath string, privateKeyPath string, password string) *ClusterSSLConfigBuilder {
	var certPEMBlock, privatePEM, der []byte
	var privKey *rsa.PrivateKey
	var cert tls.Certificate
	var err error
	if certPEMBlock, err = ioutil.ReadFile(certPath); err != nil {
		b.err = err
		return b
	}
	if privatePEM, err = ioutil.ReadFile(privateKeyPath); err != nil {
		b.err = err
		return b
	}
	privatePEMBlock, _ := pem.Decode(privatePEM)
	if der, err = x509.DecryptPEMBlock(privatePEMBlock, []byte(password)); err != nil {
		b.err = err
		return b
	}
	if privKey, err = x509.ParsePKCS1PrivateKey(der); err != nil {
		b.err = err
		return b
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: privatePEMBlock.Type, Bytes: x509.MarshalPKCS1PrivateKey(privKey)})
	if cert, err = tls.X509KeyPair(certPEMBlock, keyPEM); err != nil {
		b.err = err
		return b
	}
	b.config.TLSConfig.Certificates = append(b.config.TLSConfig.Certificates, cert)
	return b
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
		return nil, b.err
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
