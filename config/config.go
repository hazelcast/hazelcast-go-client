// Config package contains all the configuration to start a Hazelcast Instance
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

// ClientConfig is the main configuration to setup a Hazelcast Client.
type ClientConfig struct {
	// membershipListeners is the array of cluster membership listeners.
	membershipListeners []interface{}

	// lifecycleListeners is the array of listeners for listening to lifecycle events of the Hazelcast Instance.
	lifecycleListeners []interface{}

	// groupConfig is the configuration for Hazelcast groups.
	groupConfig *GroupConfig

	// clientNetworkConfig is the network configuration of the client.
	clientNetworkConfig *ClientNetworkConfig

	// serializationConfig is the serialization configuration of the client.
	serializationConfig *SerializationConfig
}

// NewClientConfig creates a new ClientConfig with default configuration.
func NewClientConfig() *ClientConfig {
	return &ClientConfig{groupConfig: NewGroupConfig(),
		clientNetworkConfig: NewClientNetworkConfig(),
		membershipListeners: make([]interface{}, 0),
		serializationConfig: NewSerializationConfig(),
		lifecycleListeners:  make([]interface{}, 0),
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

// SerializationConfig contains the serialization configuration of a Hazelcast Instance.
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

// SetPortableVersion sets the portable version.
func (serializationConfig *SerializationConfig) SetPortableVersion(version int32) {
	serializationConfig.portableVersion = version
}

// AddCustomSerializer adds a custom serializer for a given type. It can be an interface type or a struct type.
func (serializationConfig *SerializationConfig) AddCustomSerializer(typ reflect.Type, serializer Serializer) error {
	if serializer.Id() > 0 {
		serializationConfig.customSerializers[typ] = serializer
	} else {
		return common.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

// SetGlobalSerializer sets the global serializer.
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
