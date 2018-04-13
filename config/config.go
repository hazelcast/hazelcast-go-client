// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Config package contains all the configuration to start a Hazelcast instance.
package config

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"time"
)

const (
	DefaultGroupName         = "dev"
	DefaultGroupPassword     = "dev-pass"
	DefaultInvocationTimeout = 120 * time.Second //secs
)

// Config is the main configuration to setup a Hazelcast client.
type Config struct {
	// membershipListeners is the array of cluster membership listeners.
	membershipListeners []interface{}

	// lifecycleListeners is the array of listeners for listening to lifecycle events of the Hazelcast instance.
	lifecycleListeners []interface{}

	// groupConfig is the configuration for Hazelcast groups.
	groupConfig *GroupConfig

	// networkConfig is the network configuration of the client.
	networkConfig *NetworkConfig

	// serializationConfig is the serialization configuration of the client.
	serializationConfig *SerializationConfig

	// heartbeatTimeoutInSeconds is the timeout value for heartbeat in seconds.
	heartbeatTimeoutInSeconds int32

	// heartbeatIntervalInSeconds is heartbeat internal in seconds.
	heartbeatIntervalInSeconds int32

	// flakeIdGeneratorConfigMap is mapping of names to flakeIdGeneratorConfigs
	flakeIdGeneratorConfigMap map[string]*FlakeIdGeneratorConfig
}

// NewConfig creates a new Config with default configuration.
func NewConfig() *Config {
	return &Config{groupConfig: NewGroupConfig(),
		networkConfig:       NewNetworkConfig(),
		membershipListeners:       make([]interface{}, 0),
		serializationConfig:       NewSerializationConfig(),
		lifecycleListeners:        make([]interface{}, 0),
		flakeIdGeneratorConfigMap: make(map[string]*FlakeIdGeneratorConfig, 0),
	}
}

// MembershipListeners returns membership listeners.
func (config *Config) MembershipListeners() []interface{} {
	return config.membershipListeners
}

// LifecycleListeners returns lifecycle listeners.
func (config *Config) LifecycleListeners() []interface{} {
	return config.lifecycleListeners
}

// GroupConfig returns GroupConfig.
func (config *Config) GroupConfig() *GroupConfig {
	return config.groupConfig
}

// NetworkConfig returns NetworkConfig.
func (config *Config) NetworkConfig() *NetworkConfig {
	return config.networkConfig
}

// SerializationConfig returns SerializationConfig.
func (config *Config) SerializationConfig() *SerializationConfig {
	return config.serializationConfig
}

// SetHeartbeatTimeout sets the heartbeat timeout value to given timeout value.
// timeout value is in seconds.
func (config *Config) SetHeartbeatTimeoutInSeconds(timeoutInSeconds int32) *Config {
	config.heartbeatTimeoutInSeconds = timeoutInSeconds
	return config
}

// HeartbeatTimeout returns heartbeat timeout in seconds.
func (config *Config) HeartbeatTimeout() int32 {
	return config.heartbeatTimeoutInSeconds
}

// SetHeartbeatIntervalInSeconds sets the heartbeat timeout value to given interval value.
// interval value is in seconds.
func (config *Config) SetHeartbeatIntervalInSeconds(intervalInSeconds int32) *Config {
	config.heartbeatIntervalInSeconds = intervalInSeconds
	return config
}

// HeartbeatInterval returns heartbeat interval in seconds.
func (config *Config) HeartbeatInterval() int32 {
	return config.heartbeatIntervalInSeconds
}

// GetFlakeIdGeneratorConfig returns the FlakeIdGeneratorConfig for the given name, creating one
// if necessary and adding it to the map of known configurations.
// If no configuration is found with the given name it will create a new one with the default config.
func (config *Config) GetFlakeIdGeneratorConfig(name string) *FlakeIdGeneratorConfig {
	//TODO:: add config pattern matcher
	if config, found := config.flakeIdGeneratorConfigMap[name]; found {
		return config
	}
	defConfig, found := config.flakeIdGeneratorConfigMap["default"]
	if !found {
		defConfig = NewFlakeIdGeneratorConfig("default")
		config.flakeIdGeneratorConfigMap["default"] = defConfig
	}
	config := NewFlakeIdGeneratorConfig(name)
	config.flakeIdGeneratorConfigMap[name] = config
	return config

}

// AddFlakeIdGeneratorConfig adds the given config to the configurations map.
func (config *Config) AddFlakeIdGeneratorConfig(config *FlakeIdGeneratorConfig) *Config {
	config.flakeIdGeneratorConfigMap[config.Name()] = config
	return config
}

// AddMembershipListener adds a membership listener.
func (config *Config) AddMembershipListener(listener interface{}) {
	config.membershipListeners = append(config.membershipListeners, listener)
}

// AddLifecycleListener adds a lifecycle listener.
func (config *Config) AddLifecycleListener(listener interface{}) {
	config.lifecycleListeners = append(config.lifecycleListeners, listener)
}

// SetGroupConfig sets the GroupConfig.
func (config *Config) SetGroupConfig(groupConfig *GroupConfig) {
	config.groupConfig = groupConfig
}

// SetNetworkConfig sets the NetworkConfig.
func (config *Config) SetNetworkConfig(networkConfig *NetworkConfig) {
	config.networkConfig = networkConfig
}

// SetSerializationConfig sets the SerializationConfig.
func (config *Config) SetSerializationConfig(serializationConfig *SerializationConfig) {
	config.serializationConfig = serializationConfig
}

// SerializationConfig contains the serialization configuration of a Hazelcast instance.
type SerializationConfig struct {
	// isBigEndian is the byte order bool. If true, it means BigEndian, otherwise LittleEndian.
	isBigEndian bool

	// dataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	dataSerializableFactories map[int32]IdentifiedDataSerializableFactory

	// portableFactories is a map of factory IDs and corresponding Portable factories.
	portableFactories map[int32]PortableFactory

	// Portable version will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	portableVersion int32

	// customSerializers is a map of object types and corresponding custom serializers.
	customSerializers map[reflect.Type]Serializer

	// globalSerializer is the serializer that will be used if no other serializer is applicable.
	globalSerializer Serializer

	// classDefinitions contains ClassDefinitions for portable structs.
	classDefinitions []ClassDefinition
}

// NewSerializationConfig creates a SerializationConfig with default values.
func NewSerializationConfig() *SerializationConfig {
	return &SerializationConfig{isBigEndian: true, dataSerializableFactories: make(map[int32]IdentifiedDataSerializableFactory),
		portableFactories: make(map[int32]PortableFactory), portableVersion: 0, customSerializers: make(map[reflect.Type]Serializer)}
}

// IsBigEndian returns isBigEndian bool value.
func (serializationConfig *SerializationConfig) IsBigEndian() bool {
	return serializationConfig.isBigEndian
}

// DataSerializableFactories returns a map of factory IDs and corresponding IdentifiedDataSerializable factories.
func (serializationConfig *SerializationConfig) DataSerializableFactories() map[int32]IdentifiedDataSerializableFactory {
	return serializationConfig.dataSerializableFactories
}

// PortableFactories returns a map of factory IDs and corresponding Portable factories.
func (serializationConfig *SerializationConfig) PortableFactories() map[int32]PortableFactory {
	return serializationConfig.portableFactories
}

// PortableVersion returns version of a portable struct.
func (serializationConfig *SerializationConfig) PortableVersion() int32 {
	return serializationConfig.portableVersion
}

// CustomSerializers returns a map of object types and corresponding custom serializers.
func (serializationConfig *SerializationConfig) CustomSerializers() map[reflect.Type]Serializer {
	return serializationConfig.customSerializers
}

// GlobalSerializer returns the global serializer.
func (serializationConfig *SerializationConfig) GlobalSerializer() Serializer {
	return serializationConfig.globalSerializer
}

// ClassDefinitions returns registered class definitions of portable structs.
func (serializationConfig *SerializationConfig) ClassDefinitions() []ClassDefinition {
	return serializationConfig.classDefinitions
}

// SetByteOrder sets the byte order. If true, it means BigEndian, otherwise LittleEndian.
func (serializationConfig *SerializationConfig) SetByteOrder(isBigEndian bool) {
	serializationConfig.isBigEndian = isBigEndian
}

// AddDataSerializableFactory adds a IdentifiedDataSerializableFactory for a given factory ID.
func (serializationConfig *SerializationConfig) AddDataSerializableFactory(factoryId int32, f IdentifiedDataSerializableFactory) {
	serializationConfig.dataSerializableFactories[factoryId] = f
}

// AddPortableFactory adds a PortableFactory for a given factory ID.
func (serializationConfig *SerializationConfig) AddPortableFactory(factoryId int32, pf PortableFactory) {
	serializationConfig.portableFactories[factoryId] = pf
}

// AddClassDefinition registers class definitions explicitly.
func (serializationConfig *SerializationConfig) AddClassDefinition(classDefinition ...ClassDefinition) {
	serializationConfig.classDefinitions = append(serializationConfig.classDefinitions, classDefinition...)
}

// SetPortableVersion sets the portable version.
func (serializationConfig *SerializationConfig) SetPortableVersion(version int32) {
	serializationConfig.portableVersion = version
}

// AddCustomSerializer adds a custom serializer for a given type. It can be an interface type or a struct type.
func (serializationConfig *SerializationConfig) AddCustomSerializer(typ reflect.Type, serializer Serializer) error {
	if serializer.Id() > 0 {
		serializationConfig.customSerializers[typ] = serializer
	} else {
		return core.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

// SetGlobalSerializer sets the global serializer.
func (serializationConfig *SerializationConfig) SetGlobalSerializer(serializer Serializer) error {
	if serializer.Id() > 0 {
		serializationConfig.globalSerializer = serializer
	} else {
		return core.NewHazelcastSerializationError("global serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

// GroupConfig contains the configuration for Hazelcast groups.
// With groups it is possible to create multiple clusters where each cluster has its own group and doesn't
// interfere with other clusters.
type GroupConfig struct {
	// the group name of the group.
	name string

	// the group password of the group.
	password string
}

// NewGroupConfig creates a new GroupConfig with default group name and password.
func NewGroupConfig() *GroupConfig {
	return &GroupConfig{name: DefaultGroupName, password: DefaultGroupPassword}
}

// Name returns the group name of the group.
func (groupConfig *GroupConfig) Name() string {
	return groupConfig.name
}

// Password returns the group password of the group.
func (groupConfig *GroupConfig) Password() string {
	return groupConfig.password
}

// SetName sets the group name of the group.
// SetName returns the configured GroupConfig for chaining.
func (groupConfig *GroupConfig) SetName(name string) *GroupConfig {
	groupConfig.name = name
	return groupConfig
}

// SetPassword sets the group password of the group.
// SetPassword returns the configured GroupConfig for chaining.
func (groupConfig *GroupConfig) SetPassword(password string) *GroupConfig {
	groupConfig.password = password
	return groupConfig
}

// NetworkConfig contains network related configuration parameters.
type NetworkConfig struct {
	// addresses are the candidate addresses slice that client will use to establish initial connection.
	addresses []string

	// While client is trying to connect initially to one of the members in the addresses slice, all might not be
	// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as much as defined
	// by this parameter.
	connectionAttemptLimit int32

	// connectionAttemptPeriod is the period for the next attempt to find a member to connect.
	connectionAttemptPeriod int32

	// Socket connection timeout is an int32, given in seconds.
	// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
	connectionTimeout int32

	// If true, client will redo the operations that were executing on the server when client recovers the connection after a failure.
	// This can be because of network, or simply because the member died. However it is not clear whether the
	// application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
	// retrying can cause to undesirable effects. Note that the redo can perform on any member.
	redoOperation bool

	// If true, client will route the key based operations to owner of the key at the best effort. Note that it uses a
	// cached value of partition count and doesn't guarantee that the operation will always be executed on the owner.
	// The cached table is updated every 10 seconds.
	smartRouting bool

	// The invocation timeout for sending invocation.
	invocationTimeoutInSeconds time.Duration
}

func NewNetworkConfig() *NetworkConfig {
	return &NetworkConfig{
		addresses:                  make([]string, 0),
		connectionAttemptLimit:     2,
		connectionAttemptPeriod:    3,
		connectionTimeout:          5,
		redoOperation:              false,
		smartRouting:               true,
		invocationTimeoutInSeconds: DefaultInvocationTimeout,
	}
}

// Addresses returns the slice of candidate addresses that client will use to establish initial connection.
func (networkConfig *NetworkConfig) Addresses() []string {
	return networkConfig.addresses
}

// ConnectionAttemptLimit returns connection attempt limit.
func (networkConfig *NetworkConfig) ConnectionAttemptLimit() int32 {
	return networkConfig.connectionAttemptLimit
}

// ConnectionAttemptPeriod returns the period for the next attempt to find a member to connect.
func (networkConfig *NetworkConfig) ConnectionAttemptPeriod() int32 {
	return networkConfig.connectionAttemptPeriod
}

// ConnectionTimeout returns the timeout value in seconds for nodes to accept client connection requests.
func (networkConfig *NetworkConfig) ConnectionTimeout() int32 {
	return networkConfig.connectionTimeout
}

// IsRedoOperation returns true if redo operations are enabled.
func (networkConfig *NetworkConfig) IsRedoOperation() bool {
	return networkConfig.redoOperation
}

// IsSmartRouting returns true if client is smart.
func (networkConfig *NetworkConfig) IsSmartRouting() bool {
	return networkConfig.smartRouting
}

// InvocationTimeout returns the invocation timeout in seconds.
func (networkConfig *NetworkConfig) InvocationTimeout() time.Duration {
	return networkConfig.invocationTimeoutInSeconds
}

// AddAddress adds given addresses to candidate address list that client will use to establish initial connection.
// AddAddress returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) AddAddress(addresses ...string) *NetworkConfig {
	networkConfig.addresses = append(networkConfig.addresses, addresses...)
	return networkConfig
}

// SetAddresses sets given addresses as candidate address list that client will use to establish initial connection.
// SetAddresses returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetAddresses(addresses []string) *NetworkConfig {
	networkConfig.addresses = addresses
	return networkConfig
}

// While client is trying to connect initially to one of the members in the addresses slice, all might not be
// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as much as defined
// by this parameter.
// SetConnectionAttemptLimit returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetConnectionAttemptLimit(connectionAttemptLimit int32) *NetworkConfig {
	networkConfig.connectionAttemptLimit = connectionAttemptLimit
	return networkConfig
}

// SetConnectionAttemptPeriod sets the period for the next attempt to find a member to connect in seconds.
// SetConnectionAttemptPeriod returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetConnectionAttemptPeriod(connectionAttemptPeriod int32) *NetworkConfig {
	networkConfig.connectionAttemptPeriod = connectionAttemptPeriod
	return networkConfig
}

// Socket connection timeout is an int32, given in seconds.
// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
// SetConnectionTimeout returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetConnectionTimeout(connectionTimeout int32) *NetworkConfig {
	networkConfig.connectionTimeout = connectionTimeout
	return networkConfig
}

// If true, client will redo the operations that were executing on the server when client recovers the connection after a failure.
// This can be because of network, or simply because the member died. However it is not clear whether the
// application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
// retrying can cause to undesirable effects. Note that the redo can perform on any member.
// SetRedoOperation returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetRedoOperation(redoOperation bool) *NetworkConfig {
	networkConfig.redoOperation = redoOperation
	return networkConfig
}

// If true, client will route the key based operations to owner of the key at the best effort.
// Note that it uses a cached version of partitionService and doesn't
// guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
// Default value is true.
// SetSmartRouting returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetSmartRouting(smartRouting bool) *NetworkConfig {
	networkConfig.smartRouting = smartRouting
	return networkConfig
}

// SetInvocationTimeoutInSeconds sets the invocation timeout for sending invocation.
// SetInvocationTimeoutInSeconds returns the configured NetworkConfig for chaining.
func (networkConfig *NetworkConfig) SetInvocationTimeoutInSeconds(invocationTimeoutInSeconds int32) *NetworkConfig {
	networkConfig.invocationTimeoutInSeconds = time.Duration(invocationTimeoutInSeconds) * time.Second
	return networkConfig
}
