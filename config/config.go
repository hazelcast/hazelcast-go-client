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

const (
	defaultGroupName     = "dev"
	defaultGroupPassword = "dev-pass"
)

type Properties map[string]string

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

	securityConfig *SecurityConfig

	// flakeIDGeneratorConfigMap is mapping of names to flakeIDGeneratorConfigs.
	flakeIDGeneratorConfigMap map[string]*FlakeIDGeneratorConfig

	properties Properties
}

// New returns a new Config with default configuration.
func New() *Config {
	return &Config{groupConfig: NewGroupConfig(),
		networkConfig:             NewNetworkConfig(),
		membershipListeners:       make([]interface{}, 0),
		serializationConfig:       NewSerializationConfig(),
		lifecycleListeners:        make([]interface{}, 0),
		flakeIDGeneratorConfigMap: make(map[string]*FlakeIDGeneratorConfig),
		properties:                make(Properties),
		securityConfig:            new(SecurityConfig),
	}
}

// SecurityConfig returns the security config for this client.
func (cc *Config) SecurityConfig() *SecurityConfig {
	return cc.securityConfig
}

// SetSecurityConfig sets the security config for this client.
func (cc *Config) SetSecurityConfig(securityConfig *SecurityConfig) {
	cc.securityConfig = securityConfig
}

// MembershipListeners returns membership listeners.
func (cc *Config) MembershipListeners() []interface{} {
	return cc.membershipListeners
}

// LifecycleListeners returns lifecycle listeners.
func (cc *Config) LifecycleListeners() []interface{} {
	return cc.lifecycleListeners
}

// GroupConfig returns GroupConfig.
func (cc *Config) GroupConfig() *GroupConfig {
	return cc.groupConfig
}

// NetworkConfig returns NetworkConfig.
func (cc *Config) NetworkConfig() *NetworkConfig {
	return cc.networkConfig
}

// SerializationConfig returns SerializationConfig.
func (cc *Config) SerializationConfig() *SerializationConfig {
	return cc.serializationConfig
}

// SetProperty sets a new pair of property as (name, value).
func (cc *Config) SetProperty(name string, value string) {
	cc.properties[name] = value
}

// Properties returns the properties of the config.
func (cc *Config) Properties() Properties {
	return cc.properties
}

// GetFlakeIDGeneratorConfig returns the FlakeIDGeneratorConfig for the given name, creating one
// if necessary and adding it to the map of known configurations.
// If no configuration is found with the given name it will create a new one with the default Config.
func (cc *Config) GetFlakeIDGeneratorConfig(name string) *FlakeIDGeneratorConfig {
	//TODO:: add Config pattern matcher
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
func (cc *Config) AddFlakeIDGeneratorConfig(config *FlakeIDGeneratorConfig) {
	cc.flakeIDGeneratorConfigMap[config.Name()] = config
}

// AddMembershipListener adds a membership listener.
func (cc *Config) AddMembershipListener(listener interface{}) {
	cc.membershipListeners = append(cc.membershipListeners, listener)
}

// AddLifecycleListener adds a lifecycle listener.
func (cc *Config) AddLifecycleListener(listener interface{}) {
	cc.lifecycleListeners = append(cc.lifecycleListeners, listener)
}

// SetGroupConfig sets the GroupConfig.
func (cc *Config) SetGroupConfig(groupConfig *GroupConfig) {
	cc.groupConfig = groupConfig
}

// SetNetworkConfig sets the NetworkConfig.
func (cc *Config) SetNetworkConfig(networkConfig *NetworkConfig) {
	cc.networkConfig = networkConfig
}

// SetSerializationConfig sets the SerializationConfig.
func (cc *Config) SetSerializationConfig(serializationConfig *SerializationConfig) {
	cc.serializationConfig = serializationConfig
}
