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

////////////////////// ClientConfig //////////////////////////////

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

////////////////////// SerializationConfig //////////////////////////////

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

////////////////////// GroupConfig //////////////////////////////
type GroupConfig struct {
	name     string
	password string
}

func NewGroupConfig() *GroupConfig {
	return &GroupConfig{name: DEFAULT_GROUP_NAME, password: DEFAULT_GROUP_PASSWORD}
}

func (groupConfig *GroupConfig) Name() string {
	return groupConfig.name
}

func (groupConfig *GroupConfig) Password() string {
	return groupConfig.password
}

func (groupConfig *GroupConfig) SetName(name string) {
	groupConfig.name = name
}

func (groupConfig *GroupConfig) SetPassword(password string) {
	groupConfig.password = password
}

////////////////////// ClientNetworkConfig //////////////////////////////
type ClientNetworkConfig struct {
	addresses                  []string
	connectionAttemptLimit     int32
	connectionAttemptPeriod    int32
	connectionTimeout          int32
	redoOperation              bool
	smartRouting               bool
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

func (clientNetworkConfig *ClientNetworkConfig) Addresses() []string {
	return clientNetworkConfig.addresses
}

func (clientNetworkConfig *ClientNetworkConfig) ConnectionAttemptLimit() int32 {
	return clientNetworkConfig.connectionAttemptLimit
}

func (clientNetworkConfig *ClientNetworkConfig) ConnectionAttemptPeriod() int32 {
	return clientNetworkConfig.connectionAttemptPeriod
}

func (clientNetworkConfig *ClientNetworkConfig) ConnectionTimeout() int32 {
	return clientNetworkConfig.connectionTimeout
}

func (clientNetworkConfig *ClientNetworkConfig) IsRedoOperation() bool {
	return clientNetworkConfig.redoOperation
}

func (clientNetworkConfig *ClientNetworkConfig) IsSmartRouting() bool {
	return clientNetworkConfig.smartRouting
}

func (clientNetworkConfig *ClientNetworkConfig) InvocationTimeout() time.Duration {
	return clientNetworkConfig.invocationTimeoutInSeconds
}

func (clientNetworkConfig *ClientNetworkConfig) AddAddress(addresses ...string) *ClientNetworkConfig {
	clientNetworkConfig.addresses = append(clientNetworkConfig.addresses, addresses...)
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetAddresses(addresses []string) *ClientNetworkConfig {
	clientNetworkConfig.addresses = addresses
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetConnectionAttemptLimit(connectionAttemptLimit int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionAttemptLimit = connectionAttemptLimit
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetConnectionAttemptPeriod(connectionAttemptPeriod int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionAttemptPeriod = connectionAttemptPeriod
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetConnectionTimeout(connectionTimeout int32) *ClientNetworkConfig {
	clientNetworkConfig.connectionTimeout = connectionTimeout
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetRedoOperation(redoOperation bool) *ClientNetworkConfig {
	clientNetworkConfig.redoOperation = redoOperation
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetSmartRouting(smartRouting bool) *ClientNetworkConfig {
	clientNetworkConfig.smartRouting = smartRouting
	return clientNetworkConfig
}

func (clientNetworkConfig *ClientNetworkConfig) SetInvocationTimeoutInSeconds(invocationTimeoutInSeconds int32) {
	clientNetworkConfig.invocationTimeoutInSeconds = time.Duration(invocationTimeoutInSeconds) * time.Second
}
