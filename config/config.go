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
	DEFAULT_GROUP_NAME         = "dev"
	DEFAULT_GROUP_PASSWORD     = "dev-pass"
	DEFAULT_INVOCATION_TIMEOUT = 120 * time.Second //secs
)

// ClientConfig is the main configuration to setup a Hazelcast client.
type ClientConfig struct {
	// membershipListeners is the array of cluster membership listeners.
	membershipListeners []interface{}

	// lifecycleListeners is the array of listeners for listening to lifecycle events of the Hazelcast instance.
	lifecycleListeners []interface{}

	// groupConfig is the configuration for Hazelcast groups.
	groupConfig *GroupConfig

	// clientNetworkConfig is the network configuration of the client.
	clientNetworkConfig *ClientNetworkConfig

	// serializationConfig is the serialization configuration of the client.
	serializationConfig *SerializationConfig

	// heartbeatTimeoutInSeconds is the timeout value for heartbeat in seconds.
	heartbeatTimeoutInSeconds int32

	// heartbeatIntervalInSeconds is heartbeat internal in seconds.
	heartbeatIntervalInSeconds int32

	// flakeIdGeneratorConfigMap is mapping of names to flakeIdGeneratorConfigs
	flakeIdGeneratorConfigMap map[string]*FlakeIdGeneratorConfig
}

// NewClientConfig creates a new ClientConfig with default configuration.
func NewClientConfig() *ClientConfig {
	return &ClientConfig{groupConfig: NewGroupConfig(),
		clientNetworkConfig:       NewClientNetworkConfig(),
		membershipListeners:       make([]interface{}, 0),
		serializationConfig:       NewSerializationConfig(),
		lifecycleListeners:        make([]interface{}, 0),
		flakeIdGeneratorConfigMap: make(map[string]*FlakeIdGeneratorConfig, 0),
	}
}

// MembershipListeners returns membership listeners.
func (clientConfig *ClientConfig) MembershipListeners() []interface{} {
	return clientConfig.membershipListeners
}

// LifecycleListeners returns lifecycle listeners.
func (clientConfig *ClientConfig) LifecycleListeners() []interface{} {
	return clientConfig.lifecycleListeners
}

// GroupConfig returns GroupConfig.
func (clientConfig *ClientConfig) GroupConfig() *GroupConfig {
	return clientConfig.groupConfig
}

// ClientNetworkConfig returns ClientNetworkConfig.
func (clientConfig *ClientConfig) ClientNetworkConfig() *ClientNetworkConfig {
	return clientConfig.clientNetworkConfig
}

// SerializationConfig returns SerializationConfig.
func (clientConfig *ClientConfig) SerializationConfig() *SerializationConfig {
	return clientConfig.serializationConfig
}

// SetHeartbeatTimeout sets the heartbeat timeout value to given timeout value.
// timeout value is in seconds.
func (clientConfig *ClientConfig) SetHeartbeatTimeoutInSeconds(timeoutInSeconds int32) *ClientConfig {
	clientConfig.heartbeatTimeoutInSeconds = timeoutInSeconds
	return clientConfig
}

// HeartbeatTimeout returns heartbeat timeout in seconds.
func (clientConfig *ClientConfig) HeartbeatTimeout() int32 {
	return clientConfig.heartbeatTimeoutInSeconds
}

// SetHeartbeatIntervalInSeconds sets the heartbeat timeout value to given interval value.
// interval value is in seconds.
func (clientConfig *ClientConfig) SetHeartbeatIntervalInSeconds(intervalInSeconds int32) *ClientConfig {
	clientConfig.heartbeatIntervalInSeconds = intervalInSeconds
	return clientConfig
}

// HeartbeatInterval returns heartbeat interval in seconds.
func (clientConfig *ClientConfig) HeartbeatInterval() int32 {
	return clientConfig.heartbeatIntervalInSeconds
}

// GetFlakeIdGeneratorConfig returns the FlakeIdGeneratorConfig for the given name, creating one
// if necessary and adding it to the map of known configurations.
// If no configuration is found with the given name it will create a new one with the default config.
func (clientConfig *ClientConfig) GetFlakeIdGeneratorConfig(name string) *FlakeIdGeneratorConfig {
	//TODO:: add config pattern matcher
	if config, found := clientConfig.flakeIdGeneratorConfigMap[name]; found {
		return config
	}
	defConfig, found := clientConfig.flakeIdGeneratorConfigMap["default"]
	if !found {
		defConfig = NewFlakeIdGeneratorConfig("default")
		clientConfig.flakeIdGeneratorConfigMap["default"] = defConfig
	}
	config := NewFlakeIdGeneratorConfig(name)
	clientConfig.flakeIdGeneratorConfigMap[name] = config
	return config

}

// AddFlakeIdGeneratorConfig adds the given config to the configurations map.
func (clientConfig *ClientConfig) AddFlakeIdGeneratorConfig(config *FlakeIdGeneratorConfig) *ClientConfig {
	clientConfig.flakeIdGeneratorConfigMap[config.Name()] = config
	return clientConfig
}

// AddMembershipListener adds a membership listener.
func (clientConfig *ClientConfig) AddMembershipListener(listener interface{}) {
	clientConfig.membershipListeners = append(clientConfig.membershipListeners, listener)
}

// AddLifecycleListener adds a lifecycle listener.
func (clientConfig *ClientConfig) AddLifecycleListener(listener interface{}) {
	clientConfig.lifecycleListeners = append(clientConfig.lifecycleListeners, listener)
}

// SetGroupConfig sets the GroupConfig.
func (clientConfig *ClientConfig) SetGroupConfig(groupConfig *GroupConfig) {
	clientConfig.groupConfig = groupConfig
}

// SetClientNetworkConfig sets the ClientNetworkConfig.
func (clientConfig *ClientConfig) SetClientNetworkConfig(clientNetworkConfig *ClientNetworkConfig) {
	clientConfig.clientNetworkConfig = clientNetworkConfig
}

// SetSerializationConfig sets the SerializationConfig.
func (clientConfig *ClientConfig) SetSerializationConfig(serializationConfig *SerializationConfig) {
	clientConfig.serializationConfig = serializationConfig
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
	return &GroupConfig{name: DEFAULT_GROUP_NAME, password: DEFAULT_GROUP_PASSWORD}
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

// ClientNetworkConfig contains network related configuration parameters.
type ClientNetworkConfig struct {
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

func NewClientNetworkConfig() *ClientNetworkConfig {
	return &ClientNetworkConfig{
		addresses:                  make([]string, 0),
		connectionAttemptLimit:     2,
		connectionAttemptPeriod:    3,
		connectionTimeout:          5,
		redoOperation:              false,
		smartRouting:               true,
		invocationTimeoutInSeconds: DEFAULT_INVOCATION_TIMEOUT,
	}
}

// Addresses returns the slice of candidate addresses that client will use to establish initial connection.
func (clientNetworkConfig *ClientNetworkConfig) Addresses() []string {
	return clientNetworkConfig.addresses
}

// ConnectionAttemptLimit returns connection attempt limit.
func (clientNetworkConfig *ClientNetworkConfig) ConnectionAttemptLimit() int32 {
	return clientNetworkConfig.connectionAttemptLimit
}

// ConnectionAttemptPeriod returns the period for the next attempt to find a member to connect.
func (clientNetworkConfig *ClientNetworkConfig) ConnectionAttemptPeriod() int32 {
	return clientNetworkConfig.connectionAttemptPeriod
}

// ConnectionTimeout returns the timeout value in seconds for nodes to accept client connection requests.
func (clientNetworkConfig *ClientNetworkConfig) ConnectionTimeout() int32 {
	return clientNetworkConfig.connectionTimeout
}

// IsRedoOperation returns true if redo operations are enabled.
func (clientNetworkConfig *ClientNetworkConfig) IsRedoOperation() bool {
	return clientNetworkConfig.redoOperation
}

// IsSmartRouting returns true if client is smart.
func (clientNetworkConfig *ClientNetworkConfig) IsSmartRouting() bool {
	return clientNetworkConfig.smartRouting
}

// InvocationTimeout returns the invocation timeout in seconds.
func (clientNetworkConfig *ClientNetworkConfig) InvocationTimeout() time.Duration {
	return clientNetworkConfig.invocationTimeoutInSeconds
}

// AddAddress adds given addresses to candidate address list that client will use to establish initial connection.
// AddAddress returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) AddAddress(addresses ...string) *ClientNetworkConfig {
	clientNetworkConfig.addresses = append(clientNetworkConfig.addresses, addresses...)
	return clientNetworkConfig
}

// SetAddresses sets given addresses as candidate address list that client will use to establish initial connection.
// SetAddresses returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetAddresses(addresses []string) *ClientNetworkConfig {
	clientNetworkConfig.addresses = addresses
	return clientNetworkConfig
}

// While client is trying to connect initially to one of the members in the addresses slice, all might not be
// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as much as defined
// by this parameter.
// SetConnectionAttemptLimit returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetConnectionAttemptLimit(connectionAttemptLimit int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionAttemptLimit = connectionAttemptLimit
	return clientNetworkConfig
}

// SetConnectionAttemptPeriod sets the period for the next attempt to find a member to connect in seconds.
// SetConnectionAttemptPeriod returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetConnectionAttemptPeriod(connectionAttemptPeriod int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionAttemptPeriod = connectionAttemptPeriod
	return clientNetworkConfig
}

// Socket connection timeout is an int32, given in seconds.
// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
// SetConnectionTimeout returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetConnectionTimeout(connectionTimeout int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionTimeout = connectionTimeout
	return clientNetworkConfig
}

// If true, client will redo the operations that were executing on the server when client recovers the connection after a failure.
// This can be because of network, or simply because the member died. However it is not clear whether the
// application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
// retrying can cause to undesirable effects. Note that the redo can perform on any member.
// SetRedoOperation returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetRedoOperation(redoOperation bool) *ClientNetworkConfig {
	clientNetworkConfig.redoOperation = redoOperation
	return clientNetworkConfig
}

// If true, client will route the key based operations to owner of the key at the best effort.
// Note that it uses a cached version of partitionService and doesn't
// guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
// Default value is true.
// SetSmartRouting returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetSmartRouting(smartRouting bool) *ClientNetworkConfig {
	clientNetworkConfig.smartRouting = smartRouting
	return clientNetworkConfig
}

// SetInvocationTimeoutInSeconds sets the invocation timeout for sending invocation.
// SetInvocationTimeoutInSeconds returns the configured ClientNetworkConfig for chaining.
func (clientNetworkConfig *ClientNetworkConfig) SetInvocationTimeoutInSeconds(invocationTimeoutInSeconds int32) *ClientNetworkConfig {
	clientNetworkConfig.invocationTimeoutInSeconds = time.Duration(invocationTimeoutInSeconds) * time.Second
	return clientNetworkConfig
}
