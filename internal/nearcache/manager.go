/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/client"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type Manager struct {
	nearCaches   map[string]*NearCache
	nearCachesMu *sync.RWMutex
	ss           *serialization.Service
	rt           *ReparingTask
	lg           ilogger.LogAdaptor
	doneCh       chan struct{}
	state        int32
}

func NewManager(ic *client.Client, reconInterval, maxMiss int) *Manager {
	doneCh := make(chan struct{})
	cs := ic.ClusterService
	is := ic.InvocationService
	ss := ic.SerializationService
	ps := ic.PartitionService
	inf := ic.InvocationFactory
	lg := ic.Logger
	mf := NewInvalidationMetaDataFetcher(cs, is, inf, lg)
	uuid := ic.ConnectionManager.ClientUUID()
	rt := NewReparingTask(reconInterval, maxMiss, ss, ps, lg, mf, uuid, doneCh)
	ncm := &Manager{
		nearCaches:   map[string]*NearCache{},
		nearCachesMu: &sync.RWMutex{},
		ss:           ss,
		rt:           rt,
		lg:           lg,
		doneCh:       doneCh,
	}
	return ncm
}

func (m *Manager) Stop() {
	if atomic.CompareAndSwapInt32(&m.state, 0, 1) {
		close(m.doneCh)
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
		nc = NewNearCache(&cfg, m.ss, m.lg, m.doneCh)
		m.nearCaches[name] = nc
	}
	m.nearCachesMu.Unlock()
	return nc
}

func (m *Manager) RepairingTask() *ReparingTask {
	return m.rt
}

func (m *Manager) GetNearCacheStats() []proto.Pair {
	m.nearCachesMu.RLock()
	nameStats := make([]proto.Pair, 0, len(m.nearCaches))
	for name, nc := range m.nearCaches {
		nameStats = append(nameStats, proto.NewPair(name, nc.Stats()))
	}
	m.nearCachesMu.RUnlock()
	return nameStats
}
