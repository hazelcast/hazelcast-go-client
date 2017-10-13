package config

import (
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

const (
	DEFAULT_GROUP_NAME     = "dev"
	DEFAULT_GROUP_PASSWORD = "dev-pass"
)

type ClientConfig struct {
	MembershipListeners []interface{}
	LifecycleListeners  []interface{}
	GroupConfig         GroupConfig
	ClientNetworkConfig ClientNetworkConfig
	SerializationConfig *SerializationConfig
}

type SerializationConfig struct {
	isBigEndian               bool
	dataSerializableFactories map[int32]IdentifiedDataSerializableFactory
	portableFactories         map[int32]PortableFactory
	portableVersion           int32
	//customSerializers []
	//globalSerializer
}

func NewSerializationConfig() *SerializationConfig {
	return &SerializationConfig{isBigEndian: true, dataSerializableFactories: make(map[int32]IdentifiedDataSerializableFactory), portableFactories: make(map[int32]PortableFactory), portableVersion: 0}
}

func (c *SerializationConfig) AddDataSerializableFactory(factoryId int32, f IdentifiedDataSerializableFactory) {
	c.dataSerializableFactories[factoryId] = f
}

func (c *SerializationConfig) AddPortableFactory(factoryId int32, pf PortableFactory) {
	c.portableFactories[factoryId] = pf
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

func NewClientConfig() *ClientConfig {
	return &ClientConfig{GroupConfig: NewGroupConfig(),
		ClientNetworkConfig: NewClientNetworkConfig(),
		MembershipListeners: make([]interface{}, 0),
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

func NewGroupConfig() GroupConfig {
	return GroupConfig{Name: DEFAULT_GROUP_NAME, Password: DEFAULT_GROUP_PASSWORD}
}

type ClientNetworkConfig struct {
	Addresses *[]Address
	//The candidate address list that client will use to establish initial connection
	ConnectionAttemptLimit int32
	/*
		While client is trying to connect initially to one of the members in the addressList, all might be not
		available. Instead of giving up, throwing Error and stopping client, it will attempt to retry as much as defined
		by this parameter.
	*/
	ConnectionAttemptPeriod int32
	//Period for the next attempt to find a member to connect
	ConnectionTimeout int32
	/*
			Socket connection timeout is a float, giving in seconds, or None.
		    Setting a timeout of None disables the timeout feature and is equivalent to block the socket until it connects.
		    Setting a timeout of zero is the same as disables blocking on connect.
	*/
	RedoOperations bool
	/*
		If true, client will redo the operations that were executing on the server and client lost the connection.
		This can be because of network, or simply because the member died. However it is not clear whether the
		application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
		retrying can cause to undesirable effects. Note that the redo can perform on any member.
	*/
	SmartRouting bool
	/*
		If true, client will route the key based operations to owner of the key at the best effort. Note that it uses a
		cached value of partition count and doesn't guarantee that the operation will always be executed on the owner.
		The cached table is updated every 10 seconds.
	*/
}

func NewClientNetworkConfig() ClientNetworkConfig {
	return ClientNetworkConfig{
		Addresses:               new([]Address),
		ConnectionAttemptLimit:  2,
		ConnectionAttemptPeriod: 3,
		ConnectionTimeout:       5.0,
		RedoOperations:          false,
		SmartRouting:            true,
	}
}

type Address struct {
	Host string
	Port int32
}
