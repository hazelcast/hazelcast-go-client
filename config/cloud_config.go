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

package config

// ClientCloud is used as hazelcast.cloud configuration to let the client connect
// the cluster via hazelcast.cloud.
type ClientCloud struct {
	discoveryToken string
	enabled        bool
}

// NewClientCloud returns a clientCloud.
// ClientCloud is used as hazelcast.cloud configuration to let the client connect
// the cluster via hazelcast.cloud.
func NewClientCloud() *ClientCloud {
	return &ClientCloud{}
}

// SetDiscoveryToken sets the discovery token as the given token.
func (cc *ClientCloud) SetDiscoveryToken(discoveryToken string) {
	cc.discoveryToken = discoveryToken
}

// DiscoveryToken returns the discovery token.
func (cc *ClientCloud) DiscoveryToken() string {
	return cc.discoveryToken
}

// IsEnabled returns true if client cloud discovery is enabled, false otherwise.
func (cc *ClientCloud) IsEnabled() bool {
	return cc.enabled
}

// SetEnabled sets the enabled field of clientCloud.
func (cc *ClientCloud) SetEnabled(enabled bool) {
	cc.enabled = enabled
}
