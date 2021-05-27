/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import "fmt"

// AzureConfig contains Azure discovery configuration
type AzureConfig struct {
	// ResourceGroup is required only if InstanceMetadataAvailable is false.
	ResourceGroup string
	// ScaleSet is required only if InstanceMetadataAvailable is false.
	ScaleSet string
	// ClientID is used to get an access token.
	// Only required if InstanceMetadataAvailable is false.
	ClientID string
	// ClientSecret is used to get an access token.
	// Only required if InstanceMetadataAvailable is false.
	ClientSecret string
	// TenantID is required only if InstanceMetadataAvailable is false.
	TenantID string
	// SubscriptionID is required only if InstanceMetadataAvailable is false.
	SubscriptionID string
	// Tag is used to filter VM instances in the resource group.
	Tag string
	// HzPort is a range in the START-END format.
	// If START is missing, it is set to 5701.
	// If END is missing, it is set to 5703.
	// By default set to 5701-5703
	HzPort    string
	portRange portRange
	// Enabled enables Azure discovery if true.
	Enabled bool
	// InstanceMetadataAvailable enables automatic configuration of Azure discovery.
	// It's true by default.
	InstanceMetadataAvailable bool
	// UsePublicIP makes discovery use the public IP instead of the private IP.
	UsePublicIP bool // ResourceGroup is required only if InstanceMetadataAvailable is false.
	// UsePublicIP makes discovery use the public IP instead of the private IP.
}

// NewAzureConfig creates a new Azure configuration.
// You usually don't need this if you have created the configuration using hazelcast.NewConfig().
func NewAzureConfig() AzureConfig {
	return AzureConfig{
		InstanceMetadataAvailable: true,
		HzPort:                    "5701-5703",
		portRange:                 newPortRange(),
	}
}

// Clone gets a copy of AzureConfig.
func (c AzureConfig) Clone() AzureConfig {
	return c
}

// Validate makes sure AzureConfig is valid.
func (c *AzureConfig) Validate() error {
	pr := newPortRange()
	if err := pr.Parse(c.HzPort); err != nil {
		return fmt.Errorf("validating Azure config: %w", err)
	}
	c.portRange = pr
	return nil
}

// PortRange returns the port range of AzureConfig.
// The port range may be not current until Validate is called.
// This function is intended for internal use.
func (c AzureConfig) PortRange() portRange {
	return c.portRange
}
