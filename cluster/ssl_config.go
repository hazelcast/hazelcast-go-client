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

import (
	"crypto/tls"
)

// SSLConfig is SSL configuration for client.
// SSLConfig has tls.Config embedded in it so that users can set any field
// of tls config as they wish. SSL config also has some helpers such as SetCaPath, AddClientCertAndKeyPath to
// make configuration easier for users.
type SSLConfig struct {
	TLSConfig *tls.Config
	Enabled   bool
}

func (c *SSLConfig) clone() SSLConfig {
	return SSLConfig{
		Enabled:   c.Enabled,
		TLSConfig: c.TLSConfig.Clone(),
	}
}
