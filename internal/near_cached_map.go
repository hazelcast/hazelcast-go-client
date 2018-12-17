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
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type nearCachedMapProxy struct {
	*mapProxy
	nearCache     nearcache.NearCache
	serializeKeys bool
}

func newNearCachedMapProxy(client *HazelcastClient, serviceName string, name string) (*nearCachedMapProxy, error) {
	mapProxy, err := newMapProxy(client, serviceName, name)
	nearCachedProxy := &nearCachedMapProxy{
		mapProxy: mapProxy,
	}
	nearCachedProxy.init()
	return nearCachedProxy, err
}

func (n *nearCachedMapProxy) init() {
	nearCacheCfg := n.client.Config.NearCacheConfig()
	n.serializeKeys = nearCacheCfg.IsSerializeKeys()
	nearCacheManager := n.client.nearcacheManager
	n.nearCache = nearCacheManager.GetOrCreateNearCache(n.Name(), nearCacheCfg)

	// TODO:: registerInvalidationListener

}

func (n *nearCachedMapProxy) cachedValueInData(key interface{}) interface{} {
	return n.nearCache.Get(key)
}

func (n *nearCachedMapProxy) ContainsKey(key interface{}) (bool, error) {
	if cachedValue := n.cachedValueInData(key); cachedValue != nil {
		return true, nil
	}
	return n.mapProxy.ContainsKey(key)
}

func (n *nearCachedMapProxy) Get(key interface{}) (interface{}, error) {
	nearCacheKey, err := n.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	if cachedValue := n.cachedValueInObject(nearCacheKey); cachedValue != nil {
		return cachedValue, nil
	}
	keyData, err := n.toData(nearCacheKey)
	if err != nil {
		return nil, err
	}
	reservationID, reserved := n.nearCache.TryReserveForUpdate(nearCacheKey, keyData)
	value, err := n.mapProxy.Get(nearCacheKey)
	if err != nil {
		n.invalidateNearCacheKey(nearCacheKey)
		return nil, err
	}
	if reserved {
		value = n.tryPublishReserved(nearCacheKey, value, reservationID)
	}
	return value, nil
}

func (n *nearCachedMapProxy) Put(key interface{}, value interface{}) (interface{}, error) {
	nearCacheKey, err := n.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	previousValue, err := n.mapProxy.Put(nearCacheKey, value)
	if err != nil {
		return nil, err
	}
	n.invalidateNearCacheKey(nearCacheKey)
	return previousValue, nil
}

func (n *nearCachedMapProxy) tryPublishReserved(key, value interface{}, reservationID int64) interface{} {
	cachedValue, _ := n.nearCache.TryPublishReserved(key, value, reservationID, true)
	if cachedValue != nil {
		return cachedValue
	}
	return value
}

func (n *nearCachedMapProxy) cachedValueInObject(key interface{}) interface{} {
	value := n.nearCache.Get(key)
	if data, ok := value.(serialization.Data); ok {
		value, _ = n.toObject(data)
	}
	return value
}

func (n *nearCachedMapProxy) toNearCacheKey(key interface{}) (interface{}, error) {
	if n.serializeKeys {
		return n.toData(key)
	}
	return key, nil
}

func (n *nearCachedMapProxy) invalidateNearCacheKey(key interface{}) {
	n.nearCache.Invalidate(key)
}
