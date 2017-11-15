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
	MembershipListeners []interface{}
	LifecycleListeners  []interface{}
	ClientNetworkConfig *ClientNetworkConfig
	GroupConfig         *GroupConfig
	SerializationConfig *SerializationConfig
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

func (sc *SerializationConfig) AddDataSerializableFactory(factoryId int32, f IdentifiedDataSerializableFactory) {
	sc.dataSerializableFactories[factoryId] = f
}

func (sc *SerializationConfig) AddPortableFactory(factoryId int32, pf PortableFactory) {
	sc.portableFactories[factoryId] = pf
}

func (sc *SerializationConfig) IsBigEndian() bool {
	return sc.isBigEndian
}

func (sc *SerializationConfig) DataSerializableFactories() map[int32]IdentifiedDataSerializableFactory {
	return sc.dataSerializableFactories
}

func (sc *SerializationConfig) PortableFactories() map[int32]PortableFactory {
	return sc.portableFactories
}

func (sc *SerializationConfig) PortableVersion() int32 {
	return sc.portableVersion
}

func (sc *SerializationConfig) SetByteOrder(isBigEndian bool) {
	sc.isBigEndian = isBigEndian
}

func (sc *SerializationConfig) SetPortableVersion(version int32) {
	sc.portableVersion = version
}

func (sc *SerializationConfig) AddCustomSerializer(typ reflect.Type, serializer Serializer) error {
	if serializer.Id() > 0 {
		sc.customSerializers[typ] = serializer
	} else {
		return common.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

func (sc *SerializationConfig) CustomSerializers() map[reflect.Type]Serializer {
	return sc.customSerializers
}

func (sc *SerializationConfig) SetGlobalSerializer(serializer Serializer) error {
	if serializer.Id() > 0 {
		sc.globalSerializer = serializer
	} else {
		return common.NewHazelcastSerializationError("global serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

func (sc *SerializationConfig) GlobalSerializer() Serializer {
	return sc.globalSerializer
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{GroupConfig: NewGroupConfig(),
		ClientNetworkConfig: NewClientNetworkConfig(),
		MembershipListeners: make([]interface{}, 0),
		SerializationConfig: NewSerializationConfig(),
		LifecycleListeners:  make([]interface{}, 0),
	}
}
func (clientConfig *ClientConfig) IsSmartRouting() bool {
	return clientConfig.ClientNetworkConfig.SmartRouting
}
func (clientConfig *ClientConfig) AddMembershipListener(listener interface{}) {
	clientConfig.MembershipListeners = append(clientConfig.MembershipListeners, listener)
}
func (clientConfig *ClientConfig) AddLifecycleListener(listener interface{}) {
	clientConfig.LifecycleListeners = append(clientConfig.LifecycleListeners, listener)
}

type GroupConfig struct {
	Name     string
	Password string
}

func NewGroupConfig() *GroupConfig {
	return &GroupConfig{Name: DEFAULT_GROUP_NAME, Password: DEFAULT_GROUP_PASSWORD}
}
func (groupConfig *GroupConfig) SetName(name string) {
	groupConfig.Name = name
}
func (groupConfig *GroupConfig) SetPassword(password string) {
	groupConfig.Password = password
}

type ClientNetworkConfig struct {
	Addresses                  []string
	ConnectionAttemptLimit     int32
	ConnectionAttemptPeriod    int32
	ConnectionTimeout          int32
	RedoOperation              bool
	SmartRouting               bool
	InvocationTimeoutInSeconds time.Duration
}

func NewClientNetworkConfig() *ClientNetworkConfig {
	return &ClientNetworkConfig{
		Addresses:                  make([]string, 0),
		ConnectionAttemptLimit:     2,
		ConnectionAttemptPeriod:    3,
		ConnectionTimeout:          5.0,
		RedoOperation:              false,
		SmartRouting:               true,
		InvocationTimeoutInSeconds: DEFAULT_INVOCATION_TIMEOUT,
	}
}
func (clientNetworkConfig *ClientNetworkConfig) SetInvocationTimeoutInSeconds(invocationTimeoutInSeconds int32) {
	clientNetworkConfig.InvocationTimeoutInSeconds = time.Duration(invocationTimeoutInSeconds) * time.Second
}
func (clientNetworkConfig *ClientNetworkConfig) InvocationTimeout() time.Duration {
	return clientNetworkConfig.InvocationTimeoutInSeconds
}
func (clientNetworkConfig *ClientNetworkConfig) SetRedoOperation(RedoOperation bool) *ClientNetworkConfig {
	clientNetworkConfig.RedoOperation = RedoOperation
	return clientNetworkConfig
}
func (clientNetworkConfig *ClientNetworkConfig) IsRedoOperation() bool {
	return clientNetworkConfig.RedoOperation
}
