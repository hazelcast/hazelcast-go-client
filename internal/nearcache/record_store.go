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
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type RecordStore struct {
	stats            Stats
	maxIdleMillis    int64
	reservationID    int64
	timeToLiveMillis int64
	recordsMu        *sync.Mutex
	records          map[interface{}]*Record
	ss               *serialization.Service
	valueConverter   nearCacheRecordValueConverter
	estimator        nearCacheStorageEstimator
}

func NewRecordStore(cfg *nearcache.Config, ss *serialization.Service, rc nearCacheRecordValueConverter, se nearCacheStorageEstimator) RecordStore {
	stats := Stats{
		CreationTime: time.Now(),
	}
	return RecordStore{
		recordsMu:        &sync.Mutex{},
		records:          map[interface{}]*Record{},
		maxIdleMillis:    int64(cfg.MaxIdleSeconds * 1000),
		ss:               ss,
		timeToLiveMillis: int64(cfg.TimeToLiveSeconds * 1000),
		valueConverter:   rc,
		estimator:        se,
		stats:            stats,
	}
}

func (rs *RecordStore) Get(key interface{}) (value interface{}, found bool, err error) {
	// checkAvailable() does not apply since rs.records is always created
	key = rs.makeMapKey(key)
	rec, ok := rs.getRecord(key)
	if !ok {
		rs.incrementMisses()
		return nil, false, nil
	}
	value = rec.Value()
	if rec.ReservationID() != RecordReadPermitted && !rec.CachedAsNil() && value == nil {
		rs.incrementMisses()
		return nil, false, nil
	}
	// to be handled in another PR
	/*
	   if (staleReadDetector.isStaleRead(key, record)) {
	       invalidate(key);
	       nearCacheStats.incrementMisses();
	       return null;
	   }
	*/
	nowMS := time.Now().UnixMilli()
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

func (rs *RecordStore) Invalidate(key interface{}) {
	key = rs.makeMapKey(key)
	var canUpdateStats bool
	rs.recordsMu.Lock()
	rec, keyExists := rs.records[key]
	if keyExists {
		delete(rs.records, key)
		canUpdateStats = rec.ReservationID() == RecordReadPermitted
	}
	rs.recordsMu.Unlock()
	if canUpdateStats {
		rs.decrementOwnedEntryCount()
		rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
		rs.incrementInvalidations()
	}
	rs.incrementInvalidationRequests()
}

func (rs *RecordStore) TryPublishReserved(key, value interface{}, reservationID int64, deserialize bool) (interface{}, error) {
	key = rs.makeMapKey(key)
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

func (rs *RecordStore) DoEviction(withoutMaxSizeCheck bool) bool {
	// TODO: implement this
	return false
}

func (rs RecordStore) Stats() nearcache.Stats {
	return nearcache.Stats{
		CreationTime:                rs.stats.CreationTime,
		OwnedEntryCount:             atomic.LoadInt64(&rs.stats.OwnedEntryCount),
		OwnedEntryMemoryCost:        atomic.LoadInt64(&rs.stats.OwnedEntryMemoryCost),
		Hits:                        atomic.LoadInt64(&rs.stats.Hits),
		Misses:                      atomic.LoadInt64(&rs.stats.Misses),
		Evictions:                   atomic.LoadInt64(&rs.stats.Evictions),
		Expirations:                 atomic.LoadInt64(&rs.stats.Expirations),
		Invalidations:               atomic.LoadInt64(&rs.stats.Invalidations),
		PersistenceCount:            atomic.LoadInt64(&rs.stats.PersistenceCount),
		LastPersistenceWrittenBytes: atomic.LoadInt64(&rs.stats.LastPersistenceWrittenBytes),
		LastPersistenceKeyCount:     atomic.LoadInt64(&rs.stats.LastPersistenceKeyCount),
		LastPersistenceTime:         time.Time{},
		LastPersistenceDuration:     0,
		LastPersistenceFailure:      "",
	}
}

func (rs RecordStore) InvalidationRequests() int64 {
	return atomic.LoadInt64(&rs.stats.InvalidationRequests)
}

func (rs RecordStore) Size() int {
	rs.recordsMu.Lock()
	size := len(rs.records)
	rs.recordsMu.Unlock()
	return size
}

func (rs RecordStore) makeMapKey(key interface{}) interface{} {
	data, ok := key.(serialization.Data)
	if ok {
		// serialization.Data is not hashable, convert it to string
		return string(data)
	}
	return key
}

func (rs *RecordStore) toValue(v interface{}) (interface{}, error) {
	data, ok := v.(serialization.Data)
	if !ok {
		return v, nil
	}
	return rs.ss.ToObject(data)
}

func (rs *RecordStore) publishReservedRecord(key, value interface{}, rec *Record, reservationID int64) (*Record, error) {
	if rec.ReservationID() != reservationID {
		return rec, nil
	}
	update := rec.Value() != nil || rec.CachedAsNil()
	if update {
		rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
	}
	if err := rs.updateRecordValue(rec, value); err != nil {
		return nil, err
	}
	if value == nil {
		rec.SetCachedAsNil()
	}
	rec.SetReservationID(RecordReadPermitted)
	rs.incrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
	if !update {
		rs.incrementOwnedEntryCount()
	}
	return rec, nil
}

func (rs *RecordStore) getKeyStorageMemoryCost(key interface{}) int64 {
	keyData, ok := key.(serialization.Data)
	if !ok {
		// memory cost for non-data typed instance is not supported
		return 0
	}
	return int64(bits.UintSize/8 + keyData.DataSize())
}

func (rs *RecordStore) getRecordStorageMemoryCost(rec *Record) int64 {
	// TODO:
	return rs.estimator.GetRecordStorageMemoryCost(rec)
}

func (rs *RecordStore) getTotalStorageMemoryCost(key interface{}, rec *Record) int64 {
	return rs.getKeyStorageMemoryCost(key) + rs.estimator.GetRecordStorageMemoryCost(rec)
}

func (rs *RecordStore) incrementHits() {
	atomic.AddInt64(&rs.stats.Hits, 1)
}

func (rs *RecordStore) incrementMisses() {
	atomic.AddInt64(&rs.stats.Misses, 1)
}

func (rs *RecordStore) incrementExpirations() {
	atomic.AddInt64(&rs.stats.Expirations, 1)
}

func (rs *RecordStore) incrementOwnedEntryMemoryCost(cost int64) {
	atomic.AddInt64(&rs.stats.OwnedEntryMemoryCost, cost)
}

func (rs *RecordStore) decrementOwnedEntryMemoryCost(cost int64) {
	atomic.AddInt64(&rs.stats.OwnedEntryMemoryCost, -cost)
}

func (rs *RecordStore) incrementOwnedEntryCount() {
	atomic.AddInt64(&rs.stats.OwnedEntryCount, 1)
}

func (rs *RecordStore) decrementOwnedEntryCount() {
	atomic.AddInt64(&rs.stats.OwnedEntryCount, -1)
}

func (rs *RecordStore) incrementInvalidations() {
	atomic.AddInt64(&rs.stats.Invalidations, 1)
}

func (rs *RecordStore) incrementInvalidationRequests() {
	atomic.AddInt64(&rs.stats.InvalidationRequests, 1)
}

func (rs *RecordStore) getRecord(key interface{}) (*Record, bool) {
	rs.recordsMu.Lock()
	value, ok := rs.records[key]
	rs.recordsMu.Unlock()
	return value, ok
}

func (rs *RecordStore) recordExpired(rec *Record, nowMS int64) bool {
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

func (rs *RecordStore) canUpdateStats(rec *Record) bool {
	return rec != nil && rec.ReservationID() == RecordReadPermitted
}

func (rs *RecordStore) TryReserveForUpdate(key interface{}, keyData serialization.Data, ups UpdateSemantic) (int64, error) {
	// checkAvailable()
	// if there is no eviction configured we return if the Near Cache is full and it's a new key.
	// we have to check the key, otherwise we might lose updates on existing keys.
	/*
	   if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
	       return NOT_RESERVED;
	   }
	*/
	rid := rs.nextReservationID()
	var rec *Record
	var err error
	if ups == UpdateSemanticWriteUpdate {
		rec, err = rs.reserveForWriteUpdate(key, keyData, rid)
	} else {
		rec, err = rs.reserveForReadUpdate(key, keyData, rid)
	}
	if err != nil {
		return 0, err
	}
	if rec == nil || rec.ReservationID() != rid {
		return RecordNotReserved, nil
	}
	return rid, nil
}

func (rs *RecordStore) nextReservationID() int64 {
	return atomic.AddInt64(&rs.reservationID, 1)
}

func (rs *RecordStore) reserveForWriteUpdate(key interface{}, keyData serialization.Data, reservationID int64) (*Record, error) {
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
		return rec, nil
	}
	if rec.reservationID == RecordReadPermitted {
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

func (rs *RecordStore) reserveForReadUpdate(key interface{}, keyData serialization.Data, reservationID int64) (*Record, error) {
	key = rs.makeMapKey(key)
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

func (rs *RecordStore) createRecord(value interface{}) (*Record, error) {
	var err error
	value, err = rs.valueConverter.ConvertValue(value)
	if err != nil {
		return nil, err
	}
	created := time.Now().UnixMilli()
	expired := RecordStoreTimeNotSet
	if rs.timeToLiveMillis > 0 {
		expired = created + rs.timeToLiveMillis
	}
	return NewRecord(value, created, expired), nil
}

func (rs *RecordStore) updateRecordValue(rec *Record, value interface{}) error {
	value, err := rs.valueConverter.ConvertValue(value)
	if err != nil {
		return err
	}
	rec.SetValue(value)
	return nil
}
