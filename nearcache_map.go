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

package hazelcast

import (
	"context"
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type nearCacheMap struct {
	nc             *nearCache
	toNearCacheKey func(key interface{}) (interface{}, error)
	ss             *serialization.Service
}

func newNearCacheMap(nc *nearCache, ncc *nearcache.Config, ss *serialization.Service) (nearCacheMap, error) {
	ncm := nearCacheMap{
		nc: nc,
		ss: ss,
	}
	if ncc.InvalidateOnChange() {
		ncm.registerInvalidationListener()
	}
	if ncc.Preloader.Enabled {
		if err := ncm.preload(); err != nil {
			return nearCacheMap{}, fmt.Errorf("preloading near cache: %w", err)
		}
	}
	// toNearCacheKey returns the raw key if SerializeKeys is not true.
	if ncc.SerializeKeys {
		ncm.toNearCacheKey = func(key interface{}) (interface{}, error) {
			data, err := ss.ToData(key)
			if err != nil {
				return nil, err
			}
			return data, nil
		}
	} else {
		ncm.toNearCacheKey = func(key interface{}) (interface{}, error) {
			return key, nil
		}
	}
	return ncm, nil
}

func (ncm *nearCacheMap) registerInvalidationListener() {
	fmt.Println("IMPLEMENT ME: registerInvalidationListener")
}

func (ncm *nearCacheMap) preload() error {
	panic("implement me!")
}

func (ncm *nearCacheMap) ContainsKey(ctx context.Context, key interface{}, m *Map) (found bool, err error) {
	key, err = ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	cached, ok, err := ncm.getCachedValue(key, false)
	if err != nil {
		return false, err
	}
	if ok {
		return cached != nil, nil
	}
	return m.containsKeyFromRemote(ctx, key)
}

func (ncm *nearCacheMap) Delete(ctx context.Context, m *Map, key interface{}) error {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return err
	}
	defer ncm.nc.Invalidate(key)
	return m.deleteFromRemote(ctx, key)
}

func (ncm *nearCacheMap) Get(ctx context.Context, m *Map, key interface{}) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	cached, found, err := ncm.getCachedValue(key, true)
	if err != nil {
		return nil, err
	}
	if found {
		return cached, nil
	}
	// value not found in local cache.
	// get it from remote.
	value, err := ncm.getFromRemote(ctx, m, key)
	if err != nil {
		ncm.nc.Invalidate(key)
		return nil, err
	}
	return value, nil
}

func (ncm *nearCacheMap) Put(ctx context.Context, m *Map, key, value interface{}, ttl int64) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	defer ncm.nc.Invalidate(key)
	return m.putWithTTLFromRemote(ctx, key, value, ttl)
}

func (ncm *nearCacheMap) PutWithMaxIdle(ctx context.Context, m *Map, key, value interface{}, ttl int64, maxIdle int64) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	defer ncm.nc.Invalidate(key)
	return m.putWithMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

func (ncm *nearCacheMap) Remove(ctx context.Context, m *Map, key interface{}) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	defer ncm.nc.Invalidate(key)
	return m.removeFromRemote(ctx, key)
}

func (ncm *nearCacheMap) RemoveIfSame(ctx context.Context, m *Map, key interface{}, value interface{}) (bool, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	defer ncm.nc.Invalidate(key)
	return m.removeIfSameFromRemote(ctx, key, value)
}

func (ncm *nearCacheMap) Set(ctx context.Context, m *Map, key, value interface{}, ttl int64) error {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return err
	}
	defer ncm.nc.Invalidate(key)
	return m.setFromRemote(ctx, key, value, ttl)
}

func (ncm *nearCacheMap) SetWithTTLAndMaxIdle(ctx context.Context, m *Map, key, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return err
	}
	defer ncm.nc.Invalidate(key)
	return m.setWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

func (ncm *nearCacheMap) TryRemove(ctx context.Context, m *Map, key interface{}, timeout int64) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	defer ncm.nc.Invalidate(key)
	return m.tryRemoveFromRemote(ctx, key, timeout)
}

func (ncm *nearCacheMap) GetLocalMapStats() LocalMapStats {
	return LocalMapStats{
		NearCacheStats: ncm.nc.Stats(),
	}
}

func (ncm *nearCacheMap) getCachedValue(key interface{}, deserialize bool) (value interface{}, found bool, err error) {
	value, found, err = ncm.nc.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	if deserialize {
		data, ok := value.(serialization.Data)
		if ok {
			value, err = ncm.ss.ToObject(data)
			if err != nil {
				return nil, false, err
			}
		}
	}
	return value, true, nil
}

func (ncm *nearCacheMap) getFromRemote(ctx context.Context, m *Map, key interface{}) (interface{}, error) {
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	rid, err := ncm.nc.TryReserveForUpdate(key, keyData, nearCacheUpdateSemanticReadUpdate)
	if err != nil {
		return nil, err
	}
	value, err := m.getFromRemote(ctx, keyData)
	if err != nil {
		return nil, err
	}
	if rid != nearCacheRecordNotReserved {
		value, err = ncm.nc.TryPublishReserved(key, value, rid)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}