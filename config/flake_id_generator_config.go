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

const (
	defaultPrefetchCount          = 100
	defaultPrefetchValidityMillis = 600000
	maximumPrefetchCount          = 100000
)

// FlakeIdGeneratorConfig contains the configuration for 'FlakeIdGenerator' proxy.
type FlakeIdGeneratorConfig struct {
	name                   string
	prefetchCount          int32
	prefetchValidityMillis int64
}

// NewFlakeIdGeneratorConfig returns a new FlakeIdGeneratorConfig with the given name and default parameters.
func NewFlakeIdGeneratorConfig(name string) *FlakeIdGeneratorConfig {
	return &FlakeIdGeneratorConfig{
		name:                   name,
		prefetchCount:          defaultPrefetchCount,
		prefetchValidityMillis: defaultPrefetchValidityMillis,
	}
}

// NewFlakeIdGeneratorConfigWithParameters returns a new FlakeIdGeneratorConfig with the given name, prefetchCount and
// prefetchValidityMillis.
func NewFlakeIdGeneratorConfigWithParameters(name string, prefetchCount int32, prefetchValidityMillis int64) *FlakeIdGeneratorConfig {
	return &FlakeIdGeneratorConfig{
		name:                   name,
		prefetchCount:          prefetchCount,
		prefetchValidityMillis: prefetchValidityMillis,
	}
}

// SetPrefetchCount sets prefetchCount as the given value.
// prefetch count should be between 0 and maximumPrefetchCount, otherwise it
// will not be changed.
// SetPrefetchCount returns itself for chaining.
func (self *FlakeIdGeneratorConfig) SetPrefetchCount(prefetchCount int32) *FlakeIdGeneratorConfig {
	if prefetchCount > 0 && prefetchCount < maximumPrefetchCount {
		self.prefetchCount = prefetchCount
	}
	return self
}

// SetName sets the name as the given name.
// SetName returns itself for chaining.
func (self *FlakeIdGeneratorConfig) SetName(name string) *FlakeIdGeneratorConfig {
	self.name = name
	return self
}

// Name returns the name.
func (self *FlakeIdGeneratorConfig) Name() string {
	return self.name
}

// PrefetchCount returns the prefetchCount.
func (self *FlakeIdGeneratorConfig) PrefetchCount() int32 {
	return self.prefetchCount
}

// PrefetchValidityMillis returns the prefetchValidityMillis
func (self *FlakeIdGeneratorConfig) PrefetchValidityMillis() int64 {
	return self.prefetchValidityMillis
}

// SetPrefetchValidityMillis sets the prefetchValidityMillis as the given value.
// SetPrefetchValidityMillis returns itself for chaining.
func (self *FlakeIdGeneratorConfig) SetPrefetchValidityMillis(prefetchValidityMillis int64) *FlakeIdGeneratorConfig {
	self.prefetchValidityMillis = prefetchValidityMillis
	return self
}
