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
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type nearCacheManager struct {
	nearCaches   map[string]*nearCache
	nearCachesMu *sync.RWMutex
	ss           *serialization.Service
}

func newNearCacheManager(ss *serialization.Service) nearCacheManager {
	return nearCacheManager{
		nearCaches:   map[string]*nearCache{},
		nearCachesMu: &sync.RWMutex{},
		ss:           ss,
	}
}

func (m *nearCacheManager) GetOrCreateNearCache(name string, cfg nearcache.Config) *nearCache {
	m.nearCachesMu.RLock()
	nc, ok := m.nearCaches[name]
	m.nearCachesMu.RUnlock()
	if ok {
		return nc
	}
	m.nearCachesMu.Lock()
	nc, ok = m.nearCaches[name]
	if !ok {
		nc = newNearCache(&cfg, m.ss)
		m.nearCaches[name] = nc
	}
	m.nearCachesMu.Unlock()
	return nc
}

type nearCacheUpdateSemantic int8

const (
	nearCacheUpdateSemanticReadUpdate nearCacheUpdateSemantic = iota
	nearCacheUpdateSemanticWriteUpdate
)

type nearCache struct {
	cfg   *nearcache.Config
	store nearCacheRecordStore
}

func newNearCache(cfg *nearcache.Config, ss *serialization.Service) *nearCache {
	return &nearCache{
		cfg:   cfg,
		store: newNearCacheRecordStore(cfg, ss),
	}
}

func (nc *nearCache) Get(key interface{}) (interface{}, bool, error) {
	/*
		_, ok := key.(serialization.Data)
		if nc.cfg.SerializeKeys {
			if !ok {
				panic("key must be of type serialization.Data!")
			}
		} else if ok {
			panic("key cannot be of type Data!")
		}
	*/
	return nc.store.Get(key)
}

func (nc *nearCache) Invalidate(key interface{}) {
	nc.store.Invalidate(key)
}

func (nc *nearCache) Put(key interface{}, keyData serialization.Data, value interface{}, valueData serialization.Data) error {
	nc.store.DoEviction(false)
	return nc.store.Put(key, keyData, value, valueData)
}

func (nc nearCache) Size() int {
	return nc.store.Size()
}

func (nc nearCache) Stats() nearcache.Stats {
	return nc.store.Stats()
}

func (nc *nearCache) TryReserveForUpdate(key interface{}, keyData serialization.Data, ups nearCacheUpdateSemantic) (int64, error) {
	// nearCacheRecordStore.doEviction(false);
	return nc.store.TryReserveForUpdate(key, keyData, ups)
}

func (nc *nearCache) TryPublishReserved(key, value interface{}, reservationID int64) (interface{}, error) {
	cached, err := nc.store.TryPublishReserved(key, value, reservationID, true)
	if err != nil {
		return nil, err
	}
	if cached != nil {
		value = cached
	}
	return value, nil
}

const (
	nearCacheRecordStoreTimeNotSet int64 = -1
)

type nearCacheRecordStore struct {
	stats            nearcache.Stats
	maxIdleMillis    int64
	reservationID    int64
	timeToLiveMillis int64
	recordsMu        *sync.Mutex
	records          map[interface{}]*nearCacheRecord
	ss               *serialization.Service
	serializeValues  bool
}

func newNearCacheRecordStore(cfg *nearcache.Config, ss *serialization.Service) nearCacheRecordStore {
	return nearCacheRecordStore{
		recordsMu:        &sync.Mutex{},
		records:          map[interface{}]*nearCacheRecord{},
		maxIdleMillis:    int64(cfg.MaxIdleSeconds * 1000),
		ss:               ss,
		serializeValues:  cfg.InMemoryFormat == nearcache.InMemoryFormatBinary,
		timeToLiveMillis: int64(cfg.TimeToLiveSeconds * 1000),
	}
}

func (rs *nearCacheRecordStore) Get(key interface{}) (interface{}, bool, error) {
	// checkAvailable()
	rec, ok := rs.getRecord(key)
	if !ok {
		rs.incrementMisses()
		return nil, false, nil
	}
	value := rec.Value()
	if rec.ReservationID() != nearCacheRecordReadPermitted && !rec.CachedAsNil() && value == nil {
		rs.incrementMisses()
		return nil, false, nil
	}
	/*
	   if (staleReadDetector.isStaleRead(key, record)) {
	       invalidate(key);
	       nearCacheStats.incrementMisses();
	       return null;
	   }
	*/
	nowMS := internal.TimeMillis(time.Now())
	if rs.recordExpired(rec, nowMS) {
		rs.Invalidate(key)
		rs.incrementExpirations()
		return nil, false, nil
	}
	rec.SetLastAccessTimeMS(nowMS)
	rec.IncrementHits()
	rs.incrementHits()
	if value == nil {
		return nil, true, nil
	}
	//v, err := rs.ss.ToObject(value.(serialization.Data))
	//if err != nil {
	//	return nil, false, err
	//}
	//return v, true, nil
	return value, true, nil
}

func (rs *nearCacheRecordStore) Invalidate(key interface{}) {
	rs.recordsMu.Lock()
	delete(rs.records, key)
	rs.recordsMu.Unlock()
}

func (rs *nearCacheRecordStore) Put(key interface{}, keyData serialization.Data, value interface{}, valueData serialization.Data) error {
	rid, err := rs.TryReserveForUpdate(key, keyData, nearCacheUpdateSemanticReadUpdate)
	if err != nil {
		return err
	}
	if rid != nearCacheRecordNotReserved {
		if _, err := rs.TryPublishReserved(key, value, rid, false); err != nil {
			return err
		}
	}
	return nil
}

func (rs *nearCacheRecordStore) TryPublishReserved(key interface{}, value interface{}, reservationID int64, deserialize bool) (interface{}, error) {
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	existing, ok := rs.records[key]
	if ok {
		rec, err := rs.publishReserved(key, value, existing, reservationID)
		if err != nil {
			return nil, err
		}
		existing = rec
	}
	if !ok || !deserialize {
		return nil, nil
	}
	cached := existing.Value()
	data, ok := cached.(serialization.Data)
	if ok {
		return rs.ss.ToObject(data)
	}
	return cached, nil
}

func (rs *nearCacheRecordStore) DoEviction(withoutMaxSizeCheck bool) bool {
	// TODO: implement this
	return false
}

func (rs nearCacheRecordStore) Stats() nearcache.Stats {
	return nearcache.Stats{
		Hits:            atomic.LoadInt64(&rs.stats.Hits),
		Misses:          atomic.LoadInt64(&rs.stats.Misses),
		Expirations:     atomic.LoadInt64(&rs.stats.Expirations),
		OwnedMemoryCost: atomic.LoadInt64(&rs.stats.OwnedMemoryCost),
		OwnedEntryCount: atomic.LoadInt64(&rs.stats.OwnedEntryCount),
	}
}

func (rs nearCacheRecordStore) Size() int {
	rs.recordsMu.Lock()
	size := len(rs.records)
	rs.recordsMu.Unlock()
	return size
}

func (rs *nearCacheRecordStore) publishReserved(key, value interface{}, rec *nearCacheRecord, reservationID int64) (*nearCacheRecord, error) {
	if rec.ReservationID() != reservationID {
		return rec, nil
	}
	cost := rs.getKeyStorageMemoryCost(key) + rs.getRecordStorageMemoryCost(rec)
	update := rec.Value() != nil || rec.CachedAsNil()
	if update {
		rs.incrementOwnedEntryMemoryCost(-cost)
	}
	if rs.serializeValues {
		data, err := rs.ss.ToData(value)
		if err != nil {
			return nil, err
		}
		rec.SetValue(data)
	} else {
		//vv, err := rs.ss.ToObject(value.(serialization.Data))
		//if err != nil {
		//	return nil, err
		//}
		rec.SetValue(value)
	}
	if value == nil {
		rec.SetCachedAsNil(true)
	}
	rec.SetReservationID(nearCacheRecordReadPermitted)
	rs.incrementOwnedEntryMemoryCost(cost)
	if !update {
		rs.incrementOwnedEntryCount()
	}
	return rec, nil
}

func (rs *nearCacheRecordStore) getKeyStorageMemoryCost(key interface{}) int64 {
	keyData, ok := key.(serialization.Data)
	if !ok {
		// memory cost for non-data typed instance is not supported
		return 0
	}
	return int64(bits.UintSize/8 + keyData.DataSize())
}

func (rs *nearCacheRecordStore) getRecordStorageMemoryCost(rec *nearCacheRecord) int64 {
	// TODO:
	return 0
}

func (rs *nearCacheRecordStore) incrementHits() {
	atomic.AddInt64(&rs.stats.Hits, 1)
}

func (rs *nearCacheRecordStore) incrementMisses() {
	atomic.AddInt64(&rs.stats.Misses, 1)
}

func (rs *nearCacheRecordStore) incrementExpirations() {
	atomic.AddInt64(&rs.stats.Expirations, 1)
}

func (rs *nearCacheRecordStore) incrementOwnedEntryMemoryCost(cost int64) {
	atomic.AddInt64(&rs.stats.OwnedMemoryCost, cost)
}

func (rs *nearCacheRecordStore) incrementOwnedEntryCount() {
	atomic.AddInt64(&rs.stats.OwnedEntryCount, 1)
}

func (rs *nearCacheRecordStore) getRecord(key interface{}) (*nearCacheRecord, bool) {
	rs.recordsMu.Lock()
	value, ok := rs.records[key]
	rs.recordsMu.Unlock()
	return value, ok
}

func (rs *nearCacheRecordStore) recordExpired(rec *nearCacheRecord, nowMS int64) bool {
	if !rs.canUpdateStats(rec) {
		// A record can only be checked for expiry if its record state is READ_PERMITTED.
		// We can't check reserved records for expiry.
		return false
	}
	if rec.IsExpiredAtMS(nowMS) {
		return true
	}
	return rec.IsIdleAtMS(rs.maxIdleMillis, nowMS)
}

func (rs *nearCacheRecordStore) canUpdateStats(rec *nearCacheRecord) bool {
	return rec != nil && rec.ReservationID() == nearCacheRecordReadPermitted
}

func (rs *nearCacheRecordStore) TryReserveForUpdate(key interface{}, keyData serialization.Data, ups nearCacheUpdateSemantic) (int64, error) {
	// checkAvailable()
	// if there is no eviction configured we return if the Near Cache is full and it's a new key.
	// we have to check the key, otherwise we might lose updates on existing keys.
	/*
	   if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
	       return NOT_RESERVED;
	   }
	*/
	rid := rs.nextReservationID()
	var rec *nearCacheRecord
	var err error
	if ups == nearCacheUpdateSemanticWriteUpdate {
		rec, err = rs.reserveForWriteUpdate(key, keyData, rid)
	} else {
		rec, err = rs.reserveForReadUpdate(key, keyData, rid)
	}
	if err != nil {
		return 0, err
	}
	if rec == nil || rec.ReservationID() != rid {
		return nearCacheRecordNotReserved, nil
	}
	return rid, nil
}

func (rs *nearCacheRecordStore) nextReservationID() int64 {
	return atomic.AddInt64(&rs.reservationID, 1)
}

func (rs *nearCacheRecordStore) reserveForWriteUpdate(key interface{}, keyData serialization.Data, reservationID int64) (*nearCacheRecord, error) {
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	rec, ok := rs.records[key]
	if !ok {
		rec, err := rs.createRecord(nil)
		if err != nil {
			return nil, err
		}
		rec.SetReservationID(reservationID)
		// initInvalidationMetaData(record, key, keyData);
	}
	if rec.reservationID == nearCacheRecordReadPermitted {
		rec.SetReservationID(reservationID)
		return rec, nil
	}
	// 3. If this record is a previously reserved one, delete it.
	// Reasoning: CACHE_ON_UPDATE mode has different characteristics
	// than INVALIDATE mode when updating local near-cache. During
	// update, if CACHE_ON_UPDATE finds a previously reserved
	// record, that record is deleted. This is different from
	// INVALIDATE mode which doesn't delete previously reserved
	// record and keeps it as is. The reason for this deletion
	// is: concurrent reservation attempts. If CACHE_ON_UPDATE
	// doesn't delete previously reserved record, indefinite
	// read of stale value situation can be seen. Since we
	// don't apply invalidations which are sent from server
	// to near-cache if the source UUID of the invalidation
	// is same with the end's UUID which has near-cache on
	// it (client or server UUID which has near cache on it).
	return nil, nil
}

func (rs *nearCacheRecordStore) reserveForReadUpdate(key interface{}, keyData serialization.Data, reservationID int64) (*nearCacheRecord, error) {
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	rec, ok := rs.records[key]
	if ok {
		return rec, nil
	}
	rec, err := rs.createRecord(nil)
	if err != nil {
		return nil, err
	}
	rec.SetReservationID(reservationID)
	rs.records[key] = rec
	// initInvalidationMetaData(record, key, keyData);
	return rec, nil
}

func (rs *nearCacheRecordStore) createRecord(value interface{}) (*nearCacheRecord, error) {
	// assumes recordsMu was locked elsewhere
	var err error
	if rs.serializeValues {
		value, err = rs.ss.ToData(value)
		if err != nil {
			return nil, err
		}
	}
	created := internal.TimeMillis(time.Now())
	expired := nearCacheRecordStoreTimeNotSet
	if rs.timeToLiveMillis > 0 {
		expired = created + rs.timeToLiveMillis
	}
	return newNearCacheRecord(value, created, expired), nil
}

const (
	numberOfLongFieldTypes             = 2
	numberOfIntegerFieldTypes          = 5
	numberOfBooleanFieldTypes          = 1
	nearCacheRecordTimeNotSet          = -1
	nearCacheRecordNotReserved   int64 = -1
	nearCacheRecordReadPermitted       = -2
)

type nearCacheRecord struct {
	CreationTime         int64
	value                internal.AtomicValue
	UUID                 types.UUID
	PartitionID          int32
	lastAccessTime       int64
	ExpirationTime       int64
	InvalidationSequence int64
	reservationID        int64
	hits                 int32
	cachedAsNil          int32
}

func newNearCacheRecord(value interface{}, creationTime, expirationTime int64) *nearCacheRecord {
	av := internal.AtomicValue{}
	av.Store(&value)
	return &nearCacheRecord{
		CreationTime:   creationTime,
		value:          av,
		ExpirationTime: expirationTime,
	}
}

func (r *nearCacheRecord) Value() interface{} {
	return r.value.Load()
}

func (r *nearCacheRecord) SetValue(value interface{}) {
	r.value.Store(&value)
}

func (r *nearCacheRecord) Hits() int32 {
	return atomic.LoadInt32(&r.hits)
}

func (r *nearCacheRecord) SetHits(value int32) {
	atomic.StoreInt32(&r.hits, value)
}

func (r *nearCacheRecord) IncrementHits() {
	atomic.AddInt32(&r.hits, 1)
}

func (r *nearCacheRecord) ReservationID() int64 {
	return atomic.LoadInt64(&r.reservationID)
}

func (r *nearCacheRecord) SetReservationID(rid int64) {
	atomic.StoreInt64(&r.reservationID, rid)
}

func (r *nearCacheRecord) LastAccessTimeMS() int64 {
	return atomic.LoadInt64(&r.lastAccessTime)
}

func (r *nearCacheRecord) SetLastAccessTimeMS(ms int64) {
	atomic.StoreInt64(&r.lastAccessTime, ms)
}

func (r *nearCacheRecord) IsExpiredAtMS(ms int64) bool {
	return r.ExpirationTime > 0 && r.ExpirationTime <= ms
}

func (r *nearCacheRecord) IsIdleAtMS(maxIdleMS, nowMS int64) bool {
	if maxIdleMS <= 0 {
		return false
	}
	lat := r.LastAccessTimeMS()
	if lat > 0 {
		return lat+maxIdleMS < nowMS
	}
	return r.CreationTime+maxIdleMS < nowMS
}

func (r *nearCacheRecord) CachedAsNil() bool {
	return atomic.LoadInt32(&r.cachedAsNil) == 1
}

func (r *nearCacheRecord) SetCachedAsNil(value bool) {
	v := int32(0)
	if value {
		v = 1
	}
	atomic.StoreInt32(&r.cachedAsNil, v)
}
