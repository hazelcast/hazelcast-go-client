/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import "github.com/hazelcast/hazelcast-go-client/cluster/discovery"

type DiscoveryConfig struct {
	// Strategy is a discovery.Strategy implementation.
	// See the documentation in the discovery package.
	Strategy discovery.Strategy `json:",omitempty"`
	// UsePublicIP causes the client to use the public addresses of members instead of their private addresses, if available.
	UsePublicIP bool `json:",omitempty"`
}

func (c DiscoveryConfig) Clone() DiscoveryConfig {
	return c
}

func (c DiscoveryConfig) Validate() error {
	return nil
}
