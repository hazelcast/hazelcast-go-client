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

// CloudConfig contains configuration for Hazelcast Cloud.
type CloudConfig struct {
	// Token is the Hazelcast Cloud token.
	Token string `json:",omitempty"`
	// Enabled enables Hazelcast Cloud integration.
	Enabled bool `json:",omitempty"`
	// ExperimentalAPIBaseURL sets the Viridian API base URL.
	// You generally should leave its value unset.
	// Note that this configuration may be modified or removed anytime.
	ExperimentalAPIBaseURL string `json:",omitempty"`
}

func (h CloudConfig) Clone() CloudConfig {
	return h
}

func (h CloudConfig) Validate() error {
	return nil
}
