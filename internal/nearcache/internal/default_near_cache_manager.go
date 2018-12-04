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
	"sync"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type DefaultNearCacheManager struct {
	nearCachesMu         sync.RWMutex
	nearCaches           map[string]nearcache.NearCache
	serializationService spi.SerializationService
	properties           *property.HazelcastProperties
}

func (d *DefaultNearCacheManager) NearCache(name string) (nearcache.NearCache, bool) {
	d.nearCachesMu.RLock()
	defer d.nearCachesMu.RUnlock()
	nearCache, found := d.nearCaches[name]
	return nearCache, found
}

func (d *DefaultNearCacheManager) GetOrCreateNearCache(name string, config *config.NearCacheConfig) nearcache.NearCache {
	d.nearCachesMu.Lock()
	defer d.nearCachesMu.Unlock()
	if nearCache, found := d.nearCaches[name]; !found {
		nearCache = d.createNearCache(name, config)
		nearCache.Initialize()
		d.nearCaches[name] = nearCache
		return nearCache
	} else {
		return nearCache
	}
}

func (d *DefaultNearCacheManager) DestroyNearCache(name string) bool {
	d.nearCachesMu.Lock()
	defer d.nearCachesMu.Unlock()
	return d.destroyNearCacheWithoutLock(name)
}

func (d *DefaultNearCacheManager) destroyNearCacheWithoutLock(name string) bool {
	if nearCache, found := d.nearCaches[name]; found {
		delete(d.nearCaches, name)
		nearCache.Destroy()
		return true
	}
	return false
}

func (d *DefaultNearCacheManager) DestroyAllNearCaches() {
	d.nearCachesMu.Lock()
	defer d.nearCachesMu.Unlock()
	for name := range d.nearCaches {
		d.destroyNearCacheWithoutLock(name)
	}
}

func (d *DefaultNearCacheManager) ListAllNearCaches() []nearcache.NearCache {
	d.nearCachesMu.Lock()
	defer d.nearCachesMu.Unlock()
	nearCacheSlice := make([]nearcache.NearCache, 0)
	for _, nearCache := range d.nearCaches {
		nearCacheSlice = append(nearCacheSlice, nearCache)
	}
	return nearCacheSlice
}

func (d *DefaultNearCacheManager) createNearCache(name string, config *config.NearCacheConfig) nearcache.NearCache {
	return nil
}
