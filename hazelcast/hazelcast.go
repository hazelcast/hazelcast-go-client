// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package hazelcast

func StartNewClient() (*Client, error) {
	if client, err := NewClient(); err != nil {
		return nil, err
	} else if err = client.Start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

func StartNewClientWithConfig(configProvider ConfigProvider) (*Client, error) {
	if client, err := NewClientWithConfig(configProvider); err != nil {
		return nil, err
	} else if err = client.Start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

// NewClient creates and returns a new client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClient() (*Client, error) {
	return NewClientWithConfig(NewConfigBuilder())
}

// NewClientWithConfig creates and returns a new client with the given config.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClientWithConfig(configProvider ConfigProvider) (*Client, error) {
	if config, err := configProvider.Config(); err != nil {
		return nil, err
	} else {
		return newClient("", *config)
	}
}

func NewClientConfigBuilder() *ConfigBuilder {
	return NewConfigBuilder()
}
