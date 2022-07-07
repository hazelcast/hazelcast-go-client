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
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	inearcache "github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	eventTypeInvalidation = 1 << 8
)

type keyReservation struct {
	Key interface{}
	ID  int64
}

type nearCacheMap struct {
	nc                     *inearcache.NearCache
	toNearCacheKey         func(key interface{}) (interface{}, error)
	ss                     *serialization.Service
	rt                     *inearcache.ReparingTask
	lb                     *cluster.ConnectionListenerBinder
	lg                     logger.LogAdaptor
	invalidationListenerID atomic.Value
	serializeKeys          bool
}

func newNearCacheMap(ctx context.Context, nc *inearcache.NearCache, ss *serialization.Service, rt *inearcache.ReparingTask, lg logger.LogAdaptor, name string, lb *cluster.ConnectionListenerBinder, local bool) (nearCacheMap, error) {
	ncc := nc.Config()
	ncm := nearCacheMap{
		nc:            nc,
		ss:            ss,
		rt:            rt,
		lb:            lb,
		lg:            lg,
		serializeKeys: ncc.SerializeKeys,
	}
	if ncc.InvalidateOnChange() {
		lg.Debug(func() string {
			return fmt.Sprintf("registering invalidation listener: name: %s, local: %t", name, local)
		})
		if err := ncm.registerInvalidationListener(ctx, name, local); err != nil {
			lg.Errorf("hazelcast.newNearCacheMap: registering invalidation handler: %w", err)
		}
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

func (ncm *nearCacheMap) Destroy(ctx context.Context, name string) error {
	ncm.lg.Trace(func() string {
		return fmt.Sprintf("hazelcast.nearCacheMap.Destroy: %s", name)
	})
	s := ncm.invalidationListenerID.Load()
	if s == nil {
		return nil
	}
	// removeNearCacheInvalidationListener
	sid := s.(types.UUID)
	ncm.rt.DeregisterHandler(name)
	return ncm.lb.Remove(ctx, sid)
}

func (ncm *nearCacheMap) registerInvalidationListener(ctx context.Context, name string, local bool) error {
	// port of: com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy#registerInvalidationListener
	addMsg := codec.EncodeMapAddNearCacheInvalidationListenerRequest(name, eventTypeInvalidation, local)
	rth, err := ncm.rt.RegisterAndGetHandler(ctx, name, ncm.nc)
	if err != nil {
		return fmt.Errorf("nearCacheMap.registerInvalidationListener: %w", err)
	}
	handler := func(msg *proto.ClientMessage) {
		switch msg.Type() {
		case inearcache.EventIMapInvalidationMessageType:
			key, src, pt, sq := inearcache.DecodeMapInvalidationMsg(msg)
			if err := ncm.handleInvalidationMsg(&rth, key, src, pt, sq); err != nil {
				ncm.lg.Errorf("handling invalidation message: %w", err)
			}
		case inearcache.EventIMapBatchInvalidationMessageType:
			keys, srcs, pts, sqs := inearcache.DecodeMapBatchInvalidationMsg(msg)
			if err := ncm.handleBatchInvalidationMsg(&rth, keys, srcs, pts, sqs); err != nil {
				ncm.lg.Errorf("handling batch invalidation message: %w", err)
			}
		default:
			ncm.lg.Debug(func() string {
				return fmt.Sprintf("invalid invalidation message type: %d", msg.Type())
			})
		}
	}
	sid := types.NewUUID()
	removeMsg := codec.EncodeMapRemoveEntryListenerRequest(name, sid)
	if err := ncm.lb.Add(ctx, sid, addMsg, removeMsg, handler); err != nil {
		return err
	}
	ncm.invalidationListenerID.Store(sid)
	return nil
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

func (ncm *nearCacheMap) Evict(ctx context.Context, m *Map, key interface{}) (bool, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	defer ncm.nc.Invalidate(key)
	return m.evictFromRemote(ctx, key)
}

func (ncm *nearCacheMap) EvictAll(ctx context.Context, m *Map) error {
	ncm.nc.Clear()
	return m.evictAllFromRemote(ctx)
}

func (ncm *nearCacheMap) ExecuteOnKey(ctx context.Context, m *Map, entryProcessor interface{}, key interface{}) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	defer ncm.nc.Invalidate(key)
	return m.executeOnKeyFromRemote(ctx, entryProcessor, key)
}

func (ncm *nearCacheMap) ExecuteOnKeys(ctx context.Context, m *Map, entryProcessor interface{}, keys []interface{}) ([]interface{}, error) {
	ncKeys := make([]interface{}, len(keys))
	for i, k := range keys {
		nck, err := ncm.toNearCacheKey(k)
		if err != nil {
			return nil, err
		}
		ncKeys[i] = nck
	}
	defer func() {
		for _, nck := range ncKeys {
			ncm.nc.Invalidate(nck)
		}
	}()
	return m.executeOnKeysFromRemote(ctx, entryProcessor, keys)
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

func (ncm *nearCacheMap) GetAll(ctx context.Context, m *Map, keys []interface{}) ([]types.Entry, error) {
	// see: com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy#getAllInternal
	entries := make([]types.Entry, len(keys))
	missKeys, err := ncm.populateResultFromNearCache(keys, entries)
	if err != nil {
		return nil, fmt.Errorf("nearCacheMap.GetAll: populating result from near cache: %w", err)
	}
	if len(missKeys) == 0 {
		return entries, nil
	}
	keyDatas := make([]serialization.Data, len(keys))
	for i, k := range missKeys {
		kd, err := ncm.ss.ToData(k)
		if err != nil {
			return nil, err
		}
		keyDatas[i] = kd
	}
	resMap, err := ncm.getNearCacheReservations(keys, keyDatas)
	if err != nil {
		return nil, err
	}
	partitionToKeys, err := m.partitionToKeys(keys, false)
	if err != nil {
		return nil, err
	}
	defer ncm.releaseRemainingReservedKeys(resMap)
	pairs, err := m.getAllFromRemote(ctx, len(missKeys), partitionToKeys)
	if err != nil {
		return nil, fmt.Errorf("nearCacheMap.GetAll: getting keys from remote: %w", err)
	}
	keyCount, err := ncm.populateResultFromRemote(pairs, entries, resMap)
	if err != nil {
		return nil, err
	}
	return entries[:keyCount], nil
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

func (ncm *nearCacheMap) PutTransientWithTTL(ctx context.Context, m *Map, key interface{}, value interface{}, ttl int64) error {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return err
	}
	defer ncm.nc.Invalidate(key)
	return m.putTransientWithTTLFromRemote(ctx, key, value, ttl)
}

func (ncm *nearCacheMap) PutTransientWithTTLAndMaxIdle(ctx context.Context, m *Map, key interface{}, value interface{}, ttl int64, maxIdle int64) error {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return err
	}
	defer ncm.nc.Invalidate(key)
	return m.putTransientWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

func (ncm *nearCacheMap) PutIfAbsentWithTTL(ctx context.Context, m *Map, key interface{}, value interface{}, ttl int64) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	defer ncm.nc.Invalidate(key)
	return m.putIfAbsentWithTTLFromRemote(ctx, key, value, ttl)
}

func (ncm *nearCacheMap) PutIfAbsentWithTTLAndMaxIdle(ctx context.Context, m *Map, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return nil, err
	}
	defer ncm.nc.Invalidate(key)
	return m.putIfAbsentWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
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

func (ncm *nearCacheMap) TryPut(ctx context.Context, m *Map, key interface{}, value interface{}, timeout int64) (bool, error) {
	key, err := ncm.toNearCacheKey(key)
	if err != nil {
		return false, err
	}
	defer ncm.nc.Invalidate(key)
	return m.tryPutFromRemote(ctx, key, value, timeout)
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
	rid, err := ncm.nc.TryReserveForUpdate(key, keyData, inearcache.UpdateSemanticReadUpdate)
	if err != nil {
		return nil, err
	}
	value, err := m.getFromRemote(ctx, keyData)
	if err != nil {
		return nil, err
	}
	if rid != inearcache.RecordNotReserved {
		value, err = ncm.nc.TryPublishReserved(key, value, rid)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func (ncm *nearCacheMap) handleInvalidationMsg(rth *inearcache.RepairingHandler, key serialization.Data, source types.UUID, partition types.UUID, seq int64) error {
	ncm.lg.Trace(func() string {
		return fmt.Sprintf("nearCacheMap.handleInvalidationMsg: key: %v, source: %s, partition: %s, seq: %d",
			key, source, partition, seq)
	})
	return rth.Handle(key, source, partition, seq)
}

func (ncm *nearCacheMap) handleBatchInvalidationMsg(rth *inearcache.RepairingHandler, keys []serialization.Data, sources []types.UUID, partitions []types.UUID, seqs []int64) error {
	ncm.lg.Trace(func() string {
		return fmt.Sprintf("nearCacheMap.handleBatchInvalidationMsg: key count: %d", len(keys))
	})
	return rth.HandleBatch(keys, sources, partitions, seqs)
}

func (ncm *nearCacheMap) populateResultFromNearCache(keys []interface{}, entries []types.Entry) ([]interface{}, error) {
	// see: com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy#populateResultFromNearCache
	var missKeys []interface{}
	var i int
	for _, k := range keys {
		nk, err := ncm.toNearCacheKey(k)
		if err != nil {
			return nil, err
		}
		cached, ok, err := ncm.getCachedValue(nk, true)
		if err != nil {
			return nil, err
		}
		if ok && cached != nil {
			entries[i] = types.Entry{Key: k, Value: cached}
			i++
			continue
		}
		missKeys = append(missKeys, k)
	}
	return missKeys, nil
}

func (ncm *nearCacheMap) getNearCacheReservations(keys []interface{}, keyDatas []serialization.Data) (map[inearcache.DataString]keyReservation, error) {
	res := make(map[inearcache.DataString]keyReservation, len(keys))
	for i, k := range keys {
		kd := keyDatas[i]
		rid, err := ncm.nc.TryReserveForUpdate(k, kd, inearcache.UpdateSemanticReadUpdate)
		if err != nil {
			return nil, err
		}
		if rid != inearcache.RecordNotReserved {
			res[inearcache.DataString(kd)] = keyReservation{Key: k, ID: rid}
		}
	}
	return res, nil
}

func (ncm *nearCacheMap) releaseRemainingReservedKeys(rks map[inearcache.DataString]keyReservation) {
	// see: com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy#releaseRemainingReservedKeys
	serialize := ncm.serializeKeys
	for kds, rk := range rks {
		var key interface{}
		if serialize {
			key = kds
		} else {
			key = rk.Key
		}
		ncm.nc.Invalidate(key)
	}
}

func (ncm *nearCacheMap) populateResultFromRemote(pairs []proto.Pair, entries []types.Entry, reservations map[inearcache.DataString]keyReservation) (int, error) {
	// see: com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy#populateResultFromRemote
	// assumes entries has max capacity.
	serialize := ncm.serializeKeys
	i := len(entries) - len(pairs)
	for _, p := range pairs {
		kd := p.Key.(serialization.Data)
		k, err := ncm.ss.ToObject(kd)
		if err != nil {
			return 0, err
		}
		e := types.Entry{Key: k}
		kds := inearcache.DataString(kd)
		kr, ok := reservations[kds]
		if !ok {
			continue
		}
		if serialize {
			k = kd
		}
		vd := p.Value.(serialization.Data)
		v, err := ncm.nc.TryPublishReserved(k, vd, kr.ID)
		if err != nil {
			return 0, err
		}
		e.Value = v
		entries[i] = e
		i++
		delete(reservations, kds)
	}
	return i, nil
}
