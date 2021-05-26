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

type AzureConfig struct {
	Enabled                   bool
	InstanceMetadataAvailable bool
	ClientID                  string
	ClientSecret              string
	TenantID                  string
	SubscriptionID            string
	ResourceGroup             string
	ScaleSet                  string
	UsePublicIP               bool
	Tag                       string
	HzPort                    string
	portRange                 portRange
}

func NewAzureConfig() AzureConfig {
	return AzureConfig{
		InstanceMetadataAvailable: true,
		HzPort:                    "5701-5703",
		portRange:                 newPortRange(),
	}
}

func (c AzureConfig) Clone() AzureConfig {
	return c
}

func (c *AzureConfig) Validate() error {
	pr := newPortRange()
	if err := pr.Parse(c.HzPort); err != nil {
		return fmt.Errorf("validating Azure config: %w", err)
	}
	c.portRange = pr
	return nil
}

func (c AzureConfig) PortRange() portRange {
	return c.portRange
}
