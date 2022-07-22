//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

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

package hazelcast

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	inearcache "github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

func (ci *ClientInternal) ConnectionManager() *cluster.ConnectionManager {
	return ci.client.ic.ConnectionManager
}

func (ci *ClientInternal) DispatchService() *event.DispatchService {
	return ci.client.ic.EventDispatcher
}

func (ci *ClientInternal) InvocationService() *invocation.Service {
	return ci.client.ic.InvocationService
}

func (ci *ClientInternal) PartitionService() *cluster.PartitionService {
	return ci.client.ic.PartitionService
}

func (ci *ClientInternal) InvocationHandler() invocation.Handler {
	return ci.client.ic.InvocationHandler
}

func (ci *ClientInternal) ClusterService() *cluster.Service {
	return ci.client.ic.ClusterService
}

func (ci *ClientInternal) SerializationService() *serialization.Service {
	return ci.client.ic.SerializationService
}

func (ci *ClientInternal) NewNearCacheManager(reconInterval, maxMiss int) *inearcache.Manager {
	return inearcache.NewManager(ci.client.ic, reconInterval, maxMiss)
}

// MakeNearCacheAdapterFromMap returns the nearcache of the given map.
// It returns an interface{} instead of it.NearCacheAdapter in order not to introduce an import cycle.
func MakeNearCacheAdapterFromMap(m *Map) interface{} {
	if m.hasNearCache {
		return &NearCacheTestAdapter{m: m}
	}
	panic("hazelcast.MakeNearCacheAdapterFromMap: map has no near cache")
}

type NearCacheTestAdapter struct {
	m *Map
}

func (n *NearCacheTestAdapter) NearCache() *inearcache.NearCache {
	if n.m.hasNearCache {
		return n.m.ncm.nc
	}
	panic("hazelcast.NearCacheTestAdapter.NearCache: map has no near cache")
}

func (n NearCacheTestAdapter) Size() int {
	return n.m.ncm.nc.Size()
}

func (n NearCacheTestAdapter) Get(key interface{}) (interface{}, error) {
	return n.m.Get(context.Background(), key)
}

func (n NearCacheTestAdapter) GetFromNearCache(key interface{}) (interface{}, error) {
	key = n.ToNearCacheKey(key)
	v, ok, err := n.m.ncm.nc.Get(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (n NearCacheTestAdapter) GetRecord(key interface{}) (*inearcache.Record, bool) {
	key = n.ToNearCacheKey(key)
	return n.m.ncm.nc.GetRecord(key)
}

func (n NearCacheTestAdapter) InvalidationRequests() int64 {
	return n.m.ncm.nc.InvalidationRequests()
}

func (n NearCacheTestAdapter) ToNearCacheKey(key interface{}) interface{} {
	key, err := n.m.ncm.toNearCacheKey(key)
	if err != nil {
		panic(fmt.Errorf("hazelcast.NearCacheTestAdapter.ToNearCacheKey: %w", err))
	}
	return key
}
