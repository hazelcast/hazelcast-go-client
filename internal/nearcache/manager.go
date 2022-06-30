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

package nearcache

import (
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type Manager struct {
	nearCaches   map[string]*NearCache
	nearCachesMu *sync.RWMutex
	ss           *serialization.Service
}

func NewManager(ss *serialization.Service) Manager {
	return Manager{
		nearCaches:   map[string]*NearCache{},
		nearCachesMu: &sync.RWMutex{},
		ss:           ss,
	}
}

func (m *Manager) GetOrCreateNearCache(name string, cfg nearcache.Config) *NearCache {
	m.nearCachesMu.RLock()
	nc, ok := m.nearCaches[name]
	m.nearCachesMu.RUnlock()
	if ok {
		return nc
	}
	m.nearCachesMu.Lock()
	nc, ok = m.nearCaches[name]
	if !ok {
		nc = NewNearCache(&cfg, m.ss)
		m.nearCaches[name] = nc
	}
	m.nearCachesMu.Unlock()
	return nc
}
