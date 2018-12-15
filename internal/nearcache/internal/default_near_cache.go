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

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/store"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type DefaultNearCache struct {
	name                 string
	config               *config.NearCacheConfig
	serializationService spi.SerializationService
	properties           *property.HazelcastProperties
	serializeKeys        bool
	nearCacheRecordStore nearcache.RecordStore
}

func NewDefaultNearCache(name string, config *config.NearCacheConfig,
	service spi.SerializationService, properties *property.HazelcastProperties) *DefaultNearCache {
	return &DefaultNearCache{
		name:                 name,
		config:               config,
		properties:           properties,
		serializationService: service,
		serializeKeys:        config.IsSerializeKeys(),
	}
}

func (d *DefaultNearCache) Put(key interface{}, value interface{}) {
	d.nearCacheRecordStore.DoEviction(false)
	d.nearCacheRecordStore.Put(key, value)
}

func (d *DefaultNearCache) Initialize() {
	if d.nearCacheRecordStore == nil {
		d.nearCacheRecordStore = d.createNearCacheRecordStore(d.name, d.config)
	}
	d.nearCacheRecordStore.Initialize()
	// TODO:: schedule expiration task
}

func (d *DefaultNearCache) Destroy() {
	panic("implement me")
}

func (d *DefaultNearCache) createNearCacheRecordStore(name string, cfg *config.NearCacheConfig) nearcache.RecordStore {
	inMemoryFormat := d.config.InMemoryFormat()
	switch inMemoryFormat {
	case config.InMemoryFormatBinary:
		return store.NewNearCacheDataRecordStore(cfg, d.serializationService)
	case config.InMemoryFormatObject:
		return store.NewNearCacheObjectRecordStore(cfg, d.serializationService)
	default:
		return nil
	}
}
