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

// Package config contains all the configuration to start a Hazelcast instance.
package config

import (
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	defaultGroupName         = "dev"
	defaultGroupPassword     = "dev-pass"
	defaultInvocationTimeout = 120 * time.Second
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

	// heartbeatTimeout is the timeout value for heartbeat.
	heartbeatTimeout time.Duration

	// heartbeatInterval is heartbeat internal.
	heartbeatInterval time.Duration

	// flakeIDGeneratorConfigMap is mapping of names to flakeIDGeneratorConfigs.
	flakeIDGeneratorConfigMap map[string]*FlakeIDGeneratorConfig
}

// NewClientConfig returns a new ClientConfig with default configuration.
func NewClientConfig() *ClientConfig {
	return &ClientConfig{groupConfig: NewGroupConfig(),
		clientNetworkConfig:       NewClientNetworkConfig(),
		membershipListeners:       make([]interface{}, 0),
		serializationConfig:       NewSerializationConfig(),
		lifecycleListeners:        make([]interface{}, 0),
		flakeIDGeneratorConfigMap: make(map[string]*FlakeIDGeneratorConfig),
	}
}

// MembershipListeners returns membership listeners.
func (cc *ClientConfig) MembershipListeners() []interface{} {
	return cc.membershipListeners
}

// LifecycleListeners returns lifecycle listeners.
func (cc *ClientConfig) LifecycleListeners() []interface{} {
	return cc.lifecycleListeners
}

// GroupConfig returns GroupConfig.
func (cc *ClientConfig) GroupConfig() *GroupConfig {
	return cc.groupConfig
}

// ClientNetworkConfig returns ClientNetworkConfig.
func (cc *ClientConfig) ClientNetworkConfig() *ClientNetworkConfig {
	return cc.clientNetworkConfig
}

// SerializationConfig returns SerializationConfig.
func (cc *ClientConfig) SerializationConfig() *SerializationConfig {
	return cc.serializationConfig
}

// SetHeartbeatTimeout sets the heartbeat timeout value to given timeout value.
func (cc *ClientConfig) SetHeartbeatTimeout(heartbeatTimeout time.Duration) *ClientConfig {
	cc.heartbeatTimeout = heartbeatTimeout
	return cc
}

// HeartbeatTimeout returns heartbeat timeout
func (cc *ClientConfig) HeartbeatTimeout() time.Duration {
	return cc.heartbeatTimeout
}

// SetHeartbeatInterval sets the heartbeat timeout value to given interval value.
func (cc *ClientConfig) SetHeartbeatInterval(heartbeatInterval time.Duration) *ClientConfig {
	cc.heartbeatInterval = heartbeatInterval
	return cc
}

// HeartbeatInterval returns heartbeat interval.
func (cc *ClientConfig) HeartbeatInterval() time.Duration {
	return cc.heartbeatInterval
}

// GetFlakeIDGeneratorConfig returns the FlakeIDGeneratorConfig for the given name, creating one
// if necessary and adding it to the map of known configurations.
// If no configuration is found with the given name it will create a new one with the default config.
func (cc *ClientConfig) GetFlakeIDGeneratorConfig(name string) *FlakeIDGeneratorConfig {
	//TODO:: add config pattern matcher
	if config, found := cc.flakeIDGeneratorConfigMap[name]; found {
		return config
	}
	_, found := cc.flakeIDGeneratorConfigMap["default"]
	if !found {
		defConfig := NewFlakeIDGeneratorConfig("default")
		cc.flakeIDGeneratorConfigMap["default"] = defConfig
	}
	config := NewFlakeIDGeneratorConfig(name)
	cc.flakeIDGeneratorConfigMap[name] = config
	return config

}

// AddFlakeIDGeneratorConfig adds the given config to the configurations map.
func (cc *ClientConfig) AddFlakeIDGeneratorConfig(config *FlakeIDGeneratorConfig) {
	cc.flakeIDGeneratorConfigMap[config.Name()] = config
}

// AddMembershipListener adds a membership listener.
func (cc *ClientConfig) AddMembershipListener(listener interface{}) {
	cc.membershipListeners = append(cc.membershipListeners, listener)
}

// AddLifecycleListener adds a lifecycle listener.
func (cc *ClientConfig) AddLifecycleListener(listener interface{}) {
	cc.lifecycleListeners = append(cc.lifecycleListeners, listener)
}

// SetGroupConfig sets the GroupConfig.
func (cc *ClientConfig) SetGroupConfig(groupConfig *GroupConfig) {
	cc.groupConfig = groupConfig
}

// SetClientNetworkConfig sets the ClientNetworkConfig.
func (cc *ClientConfig) SetClientNetworkConfig(clientNetworkConfig *ClientNetworkConfig) {
	cc.clientNetworkConfig = clientNetworkConfig
}

// SetSerializationConfig sets the SerializationConfig.
func (cc *ClientConfig) SetSerializationConfig(serializationConfig *SerializationConfig) {
	cc.serializationConfig = serializationConfig
}

// SerializationConfig contains the serialization configuration of a Hazelcast instance.
type SerializationConfig struct {
	// isBigEndian is the byte order bool. If true, it means BigEndian, otherwise LittleEndian.
	isBigEndian bool

	// dataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	dataSerializableFactories map[int32]serialization.IdentifiedDataSerializableFactory

	// portableFactories is a map of factory IDs and corresponding Portable factories.
	portableFactories map[int32]serialization.PortableFactory

	// portableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	portableVersion int32

	// customSerializers is a map of object types and corresponding custom serializers.
	customSerializers map[reflect.Type]serialization.Serializer

	// globalSerializer is the serializer that will be used if no other serializer is applicable.
	globalSerializer serialization.Serializer

	// classDefinitions contains ClassDefinitions for portable structs.
	classDefinitions []serialization.ClassDefinition
}

// NewSerializationConfig returns a SerializationConfig with default values.
func NewSerializationConfig() *SerializationConfig {
	return &SerializationConfig{
		isBigEndian:               true,
		dataSerializableFactories: make(map[int32]serialization.IdentifiedDataSerializableFactory),
		portableFactories:         make(map[int32]serialization.PortableFactory),
		portableVersion:           0,
		customSerializers:         make(map[reflect.Type]serialization.Serializer),
	}
}

// IsBigEndian returns isBigEndian bool value.
func (sc *SerializationConfig) IsBigEndian() bool {
	return sc.isBigEndian
}

// DataSerializableFactories returns a map of factory IDs and corresponding IdentifiedDataSerializable factories.
func (sc *SerializationConfig) DataSerializableFactories() map[int32]serialization.IdentifiedDataSerializableFactory {
	return sc.dataSerializableFactories
}

// PortableFactories returns a map of factory IDs and corresponding Portable factories.
func (sc *SerializationConfig) PortableFactories() map[int32]serialization.PortableFactory {
	return sc.portableFactories
}

// PortableVersion returns version of a portable struct.
func (sc *SerializationConfig) PortableVersion() int32 {
	return sc.portableVersion
}

// CustomSerializers returns a map of object types and corresponding custom serializers.
func (sc *SerializationConfig) CustomSerializers() map[reflect.Type]serialization.Serializer {
	return sc.customSerializers
}

// GlobalSerializer returns the global serializer.
func (sc *SerializationConfig) GlobalSerializer() serialization.Serializer {
	return sc.globalSerializer
}

// ClassDefinitions returns registered class definitions of portable structs.
func (sc *SerializationConfig) ClassDefinitions() []serialization.ClassDefinition {
	return sc.classDefinitions
}

// SetByteOrder sets the byte order. If true, it means BigEndian, otherwise LittleEndian.
func (sc *SerializationConfig) SetByteOrder(isBigEndian bool) {
	sc.isBigEndian = isBigEndian
}

// AddDataSerializableFactory adds an IdentifiedDataSerializableFactory for a given factory ID.
func (sc *SerializationConfig) AddDataSerializableFactory(factoryID int32, f serialization.IdentifiedDataSerializableFactory) {
	sc.dataSerializableFactories[factoryID] = f
}

// AddPortableFactory adds a PortableFactory for a given factory ID.
func (sc *SerializationConfig) AddPortableFactory(factoryID int32, pf serialization.PortableFactory) {
	sc.portableFactories[factoryID] = pf
}

// AddClassDefinition registers class definitions explicitly.
func (sc *SerializationConfig) AddClassDefinition(classDefinition ...serialization.ClassDefinition) {
	sc.classDefinitions = append(sc.classDefinitions, classDefinition...)
}

// SetPortableVersion sets the portable version.
func (sc *SerializationConfig) SetPortableVersion(version int32) {
	sc.portableVersion = version
}

// AddCustomSerializer adds a custom serializer for a given type. It can be an interface type or a struct type.
func (sc *SerializationConfig) AddCustomSerializer(typ reflect.Type, serializer serialization.Serializer) error {
	if serializer.ID() > 0 {
		sc.customSerializers[typ] = serializer
	} else {
		return core.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

// SetGlobalSerializer sets the global serializer.
func (sc *SerializationConfig) SetGlobalSerializer(serializer serialization.Serializer) error {
	if serializer.ID() > 0 {
		sc.globalSerializer = serializer
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

// NewGroupConfig returns a new GroupConfig with default group name and password.
func NewGroupConfig() *GroupConfig {
	return &GroupConfig{name: defaultGroupName, password: defaultGroupPassword}
}

// Name returns the group name of the group.
func (gc *GroupConfig) Name() string {
	return gc.name
}

// Password returns the group password of the group.
func (gc *GroupConfig) Password() string {
	return gc.password
}

// SetName sets the group name of the group.
func (gc *GroupConfig) SetName(name string) {
	gc.name = name
}

// SetPassword sets the group password of the group.
// SetPassword returns the configured GroupConfig for chaining.
func (gc *GroupConfig) SetPassword(password string) *GroupConfig {
	gc.password = password
	return gc
}

// ClientNetworkConfig contains network related configuration parameters.
type ClientNetworkConfig struct {
	// addresses are the candidate addresses slice that client will use to establish initial connection.
	addresses []string

	// connectionAttemptLimit is how many times client will retry to connect to the members in the addresses slice.
	// While client is trying to connect initially to one of the members in the addresses slice, all might not be
	// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as many times as
	// defined by this parameter.
	connectionAttemptLimit int32

	// connectionAttemptPeriod is the period for the next attempt to find a member to connect.
	connectionAttemptPeriod time.Duration

	// connectionTimeout is a duration for connection timeout.
	// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
	connectionTimeout time.Duration

	// redoOperation determines if client will redo the operations that were executing on the server when client
	// recovers the connection after a failure. This can be because of network, or simply because the member died.
	// However it is not clear whether the application is performed or not. For idempotent operations this is harmless,
	// but for non idempotent ones retrying can cause to undesirable effects.
	// Note that the redo can perform on any member.
	redoOperation bool

	// smartRouting determines if client will route the key based operations to owner of the key at the best effort.
	// Note that it uses a cached value of partition count and doesn't guarantee that the operation will always be
	// executed on the owner.
	// The cached table is updated every 10 seconds.
	smartRouting bool

	// invocationTimeout is the invocation timeout for sending invocation.
	invocationTimeout time.Duration
}

// NewClientNetworkConfig returns a new ClientNetworkConfig with default configuration.
func NewClientNetworkConfig() *ClientNetworkConfig {
	return &ClientNetworkConfig{
		addresses:               make([]string, 0),
		connectionAttemptLimit:  2,
		connectionAttemptPeriod: 3 * time.Second,
		connectionTimeout:       5 * time.Second,
		redoOperation:           false,
		smartRouting:            true,
		invocationTimeout:       defaultInvocationTimeout,
	}
}

// Addresses returns the slice of candidate addresses that client will use to establish initial connection.
func (nc *ClientNetworkConfig) Addresses() []string {
	return nc.addresses
}

// ConnectionAttemptLimit returns connection attempt limit.
func (nc *ClientNetworkConfig) ConnectionAttemptLimit() int32 {
	return nc.connectionAttemptLimit
}

// ConnectionAttemptPeriod returns the period for the next attempt to find a member to connect.
func (nc *ClientNetworkConfig) ConnectionAttemptPeriod() time.Duration {
	return nc.connectionAttemptPeriod
}

// ConnectionTimeout returns the timeout value for nodes to accept client connection requests.
func (nc *ClientNetworkConfig) ConnectionTimeout() time.Duration {
	return nc.connectionTimeout
}

// IsRedoOperation returns true if redo operations are enabled.
func (nc *ClientNetworkConfig) IsRedoOperation() bool {
	return nc.redoOperation
}

// IsSmartRouting returns true if client is smart.
func (nc *ClientNetworkConfig) IsSmartRouting() bool {
	return nc.smartRouting
}

// InvocationTimeout returns the invocation timeout
func (nc *ClientNetworkConfig) InvocationTimeout() time.Duration {
	return nc.invocationTimeout
}

// AddAddress adds given addresses to candidate address list that client will use to establish initial connection.
func (nc *ClientNetworkConfig) AddAddress(addresses ...string) {
	nc.addresses = append(nc.addresses, addresses...)
}

// SetAddresses sets given addresses as candidate address list that client will use to establish initial connection.
func (nc *ClientNetworkConfig) SetAddresses(addresses []string) {
	nc.addresses = addresses
}

// SetConnectionAttemptLimit sets the connection attempt limit.
// While client is trying to connect initially to one of the members in the addresses slice, all might not be
// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as much as defined
// by this parameter.
func (nc *ClientNetworkConfig) SetConnectionAttemptLimit(connectionAttemptLimit int32) {
	nc.connectionAttemptLimit = connectionAttemptLimit
}

// SetConnectionAttemptPeriod sets the period for the next attempt to find a member to connect
func (nc *ClientNetworkConfig) SetConnectionAttemptPeriod(connectionAttemptPeriod time.Duration) {
	nc.connectionAttemptPeriod = connectionAttemptPeriod
}

// SetConnectionTimeout sets the connection timeout.
// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
func (nc *ClientNetworkConfig) SetConnectionTimeout(connectionTimeout time.Duration) {
	nc.connectionTimeout = connectionTimeout
}

// SetRedoOperation sets redoOperation.
// If true, client will redo the operations that were executing on the server when client recovers the connection after a failure.
// This can be because of network, or simply because the member died. However it is not clear whether the
// application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
// retrying can cause to undesirable effects. Note that the redo can perform on any member.
func (nc *ClientNetworkConfig) SetRedoOperation(redoOperation bool) {
	nc.redoOperation = redoOperation
}

// SetSmartRouting sets smartRouting.
// If true, client will route the key based operations to owner of the key at the best effort.
// Note that it uses a cached version of partitionService and doesn't
// guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
// Default value is true.
func (nc *ClientNetworkConfig) SetSmartRouting(smartRouting bool) {
	nc.smartRouting = smartRouting
}

// SetInvocationTimeout sets the invocation timeout for sending invocation.
func (nc *ClientNetworkConfig) SetInvocationTimeout(invocationTimeout time.Duration) {
	nc.invocationTimeout = invocationTimeout
}
