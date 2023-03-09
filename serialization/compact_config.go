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

package serialization

type CompactConfig struct {
	serializers map[string]CompactSerializer
}

func (cc *CompactConfig) Clone() CompactConfig {
	var clone CompactConfig
	m := make(map[string]CompactSerializer, len(cc.serializers))
	for k, v := range cc.serializers {
		m[k] = v
	}
	clone.serializers = m
	return clone
}

func (cc *CompactConfig) Serializers() map[string]CompactSerializer {
	return cc.serializers
}

func (cc *CompactConfig) Validate() error {
	cc.ensureSerializers()
	return nil
}

func (cc *CompactConfig) SetSerializers(serializers ...CompactSerializer) {
	cc.ensureSerializers()
	for _, ser := range serializers {
		cc.serializers[ser.TypeName()] = ser
	}
}

func (cc *CompactConfig) ensureSerializers() {
	if cc.serializers == nil {
		cc.serializers = make(map[string]CompactSerializer)
	}
}
