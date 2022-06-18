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
	store nearCacheRecordStore
	cfg   *nearcache.Config
}

func newNearCache(cfg *nearcache.Config, ss *serialization.Service) *nearCache {
	var rc nearCacheRecordValueConverter
	var se nearCacheStorageEstimator
	if cfg.InMemoryFormat == nearcache.InMemoryFormatBinary {
		adapter := nearCacheDataStoreAdapter{ss: ss}
		rc = adapter
		se = adapter
	} else {
		adapter := nearCacheValueStoreAdapter{ss: ss}
		rc = adapter
		se = adapter
	}
	return &nearCache{
		cfg:   cfg,
		store: newNearCacheRecordStore(cfg, ss, rc, se),
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

type nearCacheRecordValueConverter interface {
	ConvertValue(value interface{}) (interface{}, error)
}

type nearCacheStorageEstimator interface {
	GetRecordStorageMemoryCost(rec *nearCacheRecord) int64
}

type nearCacheDataStoreAdapter struct {
	ss *serialization.Service
}

func (n nearCacheDataStoreAdapter) ConvertValue(value interface{}) (interface{}, error) {
	if value == nil {
		// have to check value manually here,
		// otherwise n.ss.ToData returns type + nil, which is not recognized as nil
		return nil, nil
	}
	return n.ss.ToData(value)
}

func (n nearCacheDataStoreAdapter) GetRecordStorageMemoryCost(rec *nearCacheRecord) int64 {
	return 0
}

type nearCacheValueStoreAdapter struct {
	ss *serialization.Service
}

func (n nearCacheValueStoreAdapter) ConvertValue(value interface{}) (interface{}, error) {
	data, ok := value.(serialization.Data)
	if !ok {
		return value, nil
	}
	return n.ss.ToObject(data)
}

func (n nearCacheValueStoreAdapter) GetRecordStorageMemoryCost(rec *nearCacheRecord) int64 {
	return 0
}

type nearCacheRecordStore struct {
	stats            nearcache.Stats
	maxIdleMillis    int64
	reservationID    int64
	timeToLiveMillis int64
	recordsMu        *sync.Mutex
	records          map[interface{}]*nearCacheRecord
	ss               *serialization.Service
	valueConverter   nearCacheRecordValueConverter
	estimator        nearCacheStorageEstimator
}

func newNearCacheRecordStore(cfg *nearcache.Config, ss *serialization.Service, rc nearCacheRecordValueConverter, se nearCacheStorageEstimator) nearCacheRecordStore {
	return nearCacheRecordStore{
		recordsMu:     &sync.Mutex{},
		records:       map[interface{}]*nearCacheRecord{},
		maxIdleMillis: int64(cfg.MaxIdleSeconds * 1000),
		ss:            ss,
		//serializeValues:  cfg.InMemoryFormat == nearcache.InMemoryFormatBinary,
		timeToLiveMillis: int64(cfg.TimeToLiveSeconds * 1000),
		valueConverter:   rc,
		estimator:        se,
	}
}

func (rs *nearCacheRecordStore) Get(key interface{}) (value interface{}, found bool, err error) {
	// checkAvailable()
	rec, ok := rs.getRecord(key)
	if !ok {
		rs.incrementMisses()
		return nil, false, nil
	}
	value = rec.Value()
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
		// onExpire
		rs.incrementExpirations()
		return nil, false, nil
	}
	// onRecordAccess
	rec.SetLastAccessTimeMS(nowMS)
	rec.IncrementHits()
	rs.incrementHits()
	// recordToValue
	if value == nil {
		// CACHED_AS_NULL
		return nil, true, nil
	}
	value, err = rs.toValue(value)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (rs *nearCacheRecordStore) Invalidate(key interface{}) {
	var canUpdateStats bool
	rs.recordsMu.Lock()
	rec, keyExists := rs.records[key]
	if keyExists {
		delete(rs.records, key)
		canUpdateStats = rec.ReservationID() == nearCacheRecordReadPermitted
	}
	rs.recordsMu.Unlock()
	if canUpdateStats {
		rs.decrementOwnedEntryCount()
		rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
		rs.incrementInvalidations()
	}
	rs.incrementInvalidationRequests()
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
		rec, err := rs.publishReservedRecord(key, value, existing, reservationID)
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
		Hits:                 atomic.LoadInt64(&rs.stats.Hits),
		Misses:               atomic.LoadInt64(&rs.stats.Misses),
		Expirations:          atomic.LoadInt64(&rs.stats.Expirations),
		OwnedEntryMemoryCost: atomic.LoadInt64(&rs.stats.OwnedEntryMemoryCost),
		OwnedEntryCount:      atomic.LoadInt64(&rs.stats.OwnedEntryCount),
	}
}

func (rs nearCacheRecordStore) Size() int {
	rs.recordsMu.Lock()
	size := len(rs.records)
	rs.recordsMu.Unlock()
	return size
}

func (rs *nearCacheRecordStore) toValue(v interface{}) (interface{}, error) {
	data, ok := v.(serialization.Data)
	if !ok {
		return v, nil
	}
	return rs.ss.ToObject(data)
}

func (rs *nearCacheRecordStore) publishReservedRecord(key, value interface{}, rec *nearCacheRecord, reservationID int64) (*nearCacheRecord, error) {
	if rec.ReservationID() != reservationID {
		return rec, nil
	}
	cost := rs.getTotalStorageMemoryCost(key, rec)
	update := rec.Value() != nil || rec.CachedAsNil()
	if update {
		rs.incrementOwnedEntryMemoryCost(-cost)
	}
	if err := rs.updateRecordValue(rec, value); err != nil {
		return nil, err
	}
	if value == nil {
		rec.SetCachedAsNil()
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
	return rs.estimator.GetRecordStorageMemoryCost(rec)
}

func (rs *nearCacheRecordStore) getTotalStorageMemoryCost(key interface{}, rec *nearCacheRecord) int64 {
	return rs.getKeyStorageMemoryCost(key) + rs.estimator.GetRecordStorageMemoryCost(rec)
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
	atomic.AddInt64(&rs.stats.OwnedEntryMemoryCost, cost)
}

func (rs *nearCacheRecordStore) decrementOwnedEntryMemoryCost(cost int64) {
	atomic.AddInt64(&rs.stats.OwnedEntryMemoryCost, -cost)
}

func (rs *nearCacheRecordStore) incrementOwnedEntryCount() {
	atomic.AddInt64(&rs.stats.OwnedEntryCount, 1)
}

func (rs *nearCacheRecordStore) decrementOwnedEntryCount() {
	atomic.AddInt64(&rs.stats.OwnedEntryCount, -1)
}

func (rs *nearCacheRecordStore) incrementInvalidations() {
	atomic.AddInt64(&rs.stats.Invalidations, 1)
}

func (rs *nearCacheRecordStore) incrementInvalidationRequests() {
	atomic.AddInt64(&rs.stats.InvalidationRequests, 1)
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
	value, err = rs.valueConverter.ConvertValue(value)
	if err != nil {
		return nil, err
	}
	created := internal.TimeMillis(time.Now())
	expired := nearCacheRecordStoreTimeNotSet
	if rs.timeToLiveMillis > 0 {
		expired = created + rs.timeToLiveMillis
	}
	return newNearCacheRecord(value, created, expired), nil
}

func (rs *nearCacheRecordStore) updateRecordValue(rec *nearCacheRecord, value interface{}) error {
	value, err := rs.valueConverter.ConvertValue(value)
	if err != nil {
		return err
	}
	rec.SetValue(value)
	return nil
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

func (r *nearCacheRecord) SetCachedAsNil() {
	atomic.StoreInt32(&r.cachedAsNil, 1)
}
