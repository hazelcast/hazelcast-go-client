// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package config

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/serialization"
	"reflect"
	"time"
)

const (
	DEFAULT_GROUP_NAME         = "dev"
	DEFAULT_GROUP_PASSWORD     = "dev-pass"
	DEFAULT_INVOCATION_TIMEOUT = 120 * time.Second //secs
)

type ClientConfig struct {
	membershipListeners []interface{}
	lifecycleListeners  []interface{}
	groupConfig         *GroupConfig
	clientNetworkConfig *ClientNetworkConfig
	serializationConfig *SerializationConfig
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{groupConfig: NewGroupConfig(),
		clientNetworkConfig: NewClientNetworkConfig(),
		membershipListeners: make([]interface{}, 0),
		serializationConfig: NewSerializationConfig(),
		lifecycleListeners:  make([]interface{}, 0),
	}
}

func (clientConfig *ClientConfig) MembershipListeners() []interface{} {
	return clientConfig.membershipListeners
}

func (clientConfig *ClientConfig) LifecycleListeners() []interface{} {
	return clientConfig.lifecycleListeners
}

func (clientConfig *ClientConfig) GroupConfig() *GroupConfig {
	return clientConfig.groupConfig
}

func (clientConfig *ClientConfig) ClientNetworkConfig() *ClientNetworkConfig {
	return clientConfig.clientNetworkConfig
}

func (clientConfig *ClientConfig) SerializationConfig() *SerializationConfig {
	return clientConfig.serializationConfig
}

func (clientConfig *ClientConfig) AddMembershipListener(listener interface{}) {
	clientConfig.membershipListeners = append(clientConfig.membershipListeners, listener)
}

func (clientConfig *ClientConfig) AddLifecycleListener(listener interface{}) {
	clientConfig.lifecycleListeners = append(clientConfig.lifecycleListeners, listener)
}

func (clientConfig *ClientConfig) SetGroupConfig(groupConfig *GroupConfig) {
	clientConfig.groupConfig = groupConfig
}

func (clientConfig *ClientConfig) SetClientNetworkConfig(clientNetworkConfig *ClientNetworkConfig) {
	clientConfig.clientNetworkConfig = clientNetworkConfig
}

func (clientConfig *ClientConfig) SetSerializationConfig(serializationConfig *SerializationConfig) {
	clientConfig.serializationConfig = serializationConfig
}

type SerializationConfig struct {
	isBigEndian               bool
	dataSerializableFactories map[int32]IdentifiedDataSerializableFactory
	portableFactories         map[int32]PortableFactory
	portableVersion           int32
	customSerializers         map[reflect.Type]Serializer
	globalSerializer          Serializer
}

func NewSerializationConfig() *SerializationConfig {
	return &SerializationConfig{isBigEndian: true, dataSerializableFactories: make(map[int32]IdentifiedDataSerializableFactory),
		portableFactories: make(map[int32]PortableFactory), portableVersion: 0, customSerializers: make(map[reflect.Type]Serializer)}
}

func (serializationConfig *SerializationConfig) IsBigEndian() bool {
	return serializationConfig.isBigEndian
}

func (serializationConfig *SerializationConfig) DataSerializableFactories() map[int32]IdentifiedDataSerializableFactory {
	return serializationConfig.dataSerializableFactories
}

func (serializationConfig *SerializationConfig) PortableFactories() map[int32]PortableFactory {
	return serializationConfig.portableFactories
}

func (serializationConfig *SerializationConfig) PortableVersion() int32 {
	return serializationConfig.portableVersion
}

func (serializationConfig *SerializationConfig) CustomSerializers() map[reflect.Type]Serializer {
	return serializationConfig.customSerializers
}

func (serializationConfig *SerializationConfig) GlobalSerializer() Serializer {
	return serializationConfig.globalSerializer
}

func (serializationConfig *SerializationConfig) SetByteOrder(isBigEndian bool) {
	serializationConfig.isBigEndian = isBigEndian
}

func (serializationConfig *SerializationConfig) AddDataSerializableFactory(factoryId int32, f IdentifiedDataSerializableFactory) {
	serializationConfig.dataSerializableFactories[factoryId] = f
}

func (serializationConfig *SerializationConfig) AddPortableFactory(factoryId int32, pf PortableFactory) {
	serializationConfig.portableFactories[factoryId] = pf
}

func (serializationConfig *SerializationConfig) SetPortableVersion(version int32) {
	serializationConfig.portableVersion = version
}

func (serializationConfig *SerializationConfig) AddCustomSerializer(typ reflect.Type, serializer Serializer) error {
	if serializer.Id() > 0 {
		serializationConfig.customSerializers[typ] = serializer
	} else {
		return common.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

func (serializationConfig *SerializationConfig) SetGlobalSerializer(serializer Serializer) error {
	if serializer.Id() > 0 {
		serializationConfig.globalSerializer = serializer
	} else {
		return common.NewHazelcastSerializationError("global serializer should have its typeId greater than or equal to 1", nil)
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
func (groupConfig *GroupConfig) SetName(name string) {
	groupConfig.name = name
}

// SetPassword sets the group password of the group.
// SetPassword returns the configured GroupConfig for chaining.
func (groupConfig *GroupConfig) SetPassword(password string) {
	groupConfig.password = password
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
func (clientNetworkConfig *ClientNetworkConfig) SetInvocationTimeoutInSeconds(invocationTimeoutInSeconds int32) {
	clientNetworkConfig.invocationTimeoutInSeconds = time.Duration(invocationTimeoutInSeconds) * time.Second
}
