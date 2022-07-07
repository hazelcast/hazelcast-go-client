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
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

const (
	RecordNotReserved     int64 = -1
	RecordReadPermitted   int64 = -2
	RecordStoreTimeNotSet int64 = -1
	// see: com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy#SAMPLE_COUNT
	RecordStoreSampleCount = 15
)

// serialization.Data is not hashable, so using this to store keys.
type dataString string

type RecordStore struct {
	stats             nearcache.Stats
	maxIdleMillis     int64
	reservationID     int64
	timeToLiveMillis  int64
	recordsMu         *sync.RWMutex
	records           map[interface{}]*Record
	ss                *serialization.Service
	valueConverter    nearCacheRecordValueConverter
	estimator         nearCacheStorageEstimator
	staleReadDetector *StaleReadDetector
	evictionDisabled  bool
	maxSize           int
	cmp               nearcache.EvictionPolicyComparator
}

func NewRecordStore(cfg *nearcache.Config, ss *serialization.Service, rc nearCacheRecordValueConverter, se nearCacheStorageEstimator) RecordStore {
	stats := nearcache.Stats{
		CreationTime: time.Now(),
	}
	return RecordStore{
		recordsMu:        &sync.RWMutex{},
		records:          map[interface{}]*Record{},
		maxIdleMillis:    int64(cfg.MaxIdleSeconds * 1000),
		ss:               ss,
		timeToLiveMillis: int64(cfg.TimeToLiveSeconds * 1000),
		valueConverter:   rc,
		estimator:        se,
		stats:            stats,
		evictionDisabled: cfg.Eviction.EvictionPolicy() == nearcache.EvictionPolicyNone,
		maxSize:          cfg.Eviction.Size(),
		cmp:              getEvictionPolicyComparator(&cfg.Eviction),
	}
}

func (rs *RecordStore) Clear() {
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#clear
	// checkAvailable() does not apply since rs.records is always created
	rs.recordsMu.Lock()
	size := len(rs.records)
	rs.records = nil
	rs.recordsMu.Unlock()
	atomic.StoreInt64(&rs.stats.OwnedEntryCount, 0)
	atomic.StoreInt64(&rs.stats.OwnedEntryMemoryCost, 0)
	atomic.AddInt64(&rs.stats.Invalidations, int64(size))
	rs.incrementInvalidationRequests()
}

func (rs *RecordStore) Get(key interface{}) (value interface{}, found bool, err error) {
	// checkAvailable() does not apply since rs.records is always created
	key = rs.makeMapKey(key)
	rs.recordsMu.RLock()
	rec, ok := rs.getRecord(key)
	rs.recordsMu.RUnlock()
	if !ok {
		rs.incrementMisses()
		return nil, false, nil
	}
	value = rec.Value()
	if rec.ReservationID() != RecordReadPermitted && !rec.CachedAsNil() && value == nil {
		rs.incrementMisses()
		return nil, false, nil
	}
	// instead of ALWAYS_FRESH staleReadDetector, the nil value used
	if rs.staleReadDetector != nil && rs.staleReadDetector.IsStaleRead(rec) {
		rs.Invalidate(key)
		rs.incrementMisses()
		return nil, false, nil
	}
	nowMS := time.Now().UnixMilli()
	if rs.recordExpired(rec, nowMS) {
		rs.Invalidate(key)
		rs.onExpire()
		return nil, false, nil
	}
	// onRecordAccess
	rec.SetLastAccessTime(nowMS)
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

func (rs *RecordStore) GetRecord(key interface{}) (*Record, bool) {
	// this function is exported only for tests.
	// do not use outside of tests.
	rs.recordsMu.RLock()
	defer rs.recordsMu.RUnlock()
	key = rs.makeMapKey(key)
	return rs.getRecord(key)
}

func (rs *RecordStore) Invalidate(key interface{}) {
	rs.recordsMu.Lock()
	rs.invalidate(key)
	rs.recordsMu.Unlock()
}

func (rs *RecordStore) invalidate(key interface{}) {
	// assumes rs.recordsMu is locked.
	key = rs.makeMapKey(key)
	var canUpdateStats bool
	rec, exists := rs.records[key]
	if exists {
		delete(rs.records, key)
		canUpdateStats = rec.ReservationID() == RecordReadPermitted
	}
	if canUpdateStats {
		rs.decrementOwnedEntryCount()
		rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
		rs.incrementInvalidations()
	}
	rs.incrementInvalidationRequests()
}

func (rs *RecordStore) doEviction() bool {
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#doEviction
	// note that the reference implementation never has withoutMaxSizeCheck == true
	// checkAvailable doesn't apply
	if rs.evictionDisabled {
		return false
	}
	return rs.evict()
}

func (rs *RecordStore) DoExpiration() {
	// port of: com.hazelcast.internal.nearcache.impl.store.BaseHeapNearCacheRecordStore#doExpiration
	now := time.Now().UnixMilli()
	rs.recordsMu.Lock()
	for k, v := range rs.records {
		if rs.recordExpired(v, now) {
			rs.invalidate(k)
			rs.onExpire()
		}
	}
	rs.recordsMu.Unlock()
}

func (rs *RecordStore) onEvict(key interface{}, rec *Record, expired bool) {
	// port of: com.hazelcast.internal.nearcache.impl.store.BaseHeapNearCacheRecordStore#onEvict
	if expired {
		rs.incrementExpirations()
	} else {
		rs.incrementEvictions()
	}
	rs.decrementOwnedEntryCount()
	rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
}

func (rs *RecordStore) tryEvict(candidate evictionCandidate) bool {
	// port of: com.hazelcast.internal.nearcache.impl.store.HeapNearCacheRecordMap#tryEvict
	if exists := rs.remove(candidate.Key()); !exists {
		return false
	}
	rs.onEvict(candidate.key, candidate.evictable, false)
	return true
}

func (rs *RecordStore) remove(key interface{}) bool {
	// the key is already in the "made" form, so don't run on rs.makeMapKey on it
	// assumes rs.recordsMu is locked elsewhere
	if _, exists := rs.records[key]; !exists {
		return false
	}
	delete(rs.records, key)
	return true
}

func (rs *RecordStore) sample(count int) []evictionCandidate {
	// see: com.hazelcast.internal.nearcache.impl.store.HeapNearCacheRecordMap#sample
	// currently we use builtin maps of the Go client, so another random sampling algorithm is used.
	// note that count is fixed to 15 in the reference implementation, it is always positive
	// assumes recordsMu is locked
	samples := make([]evictionCandidate, count)
	var idx int
	for k, v := range rs.records {
		// access to keys of maps is random
		samples[idx] = evictionCandidate{
			key:       k,
			evictable: v,
		}
		idx++
		if idx >= count {
			break
		}
	}
	return samples
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
	rs.recordsMu.RLock()
	size := len(rs.records)
	rs.recordsMu.RUnlock()
	return size
}

func (rs RecordStore) makeMapKey(key interface{}) interface{} {
	data, ok := key.(serialization.Data)
	if ok {
		// serialization.Data is not hashable, convert it to string
		return dataString(data)
	}
	return key
}

func (rs RecordStore) unMakeMapKey(key interface{}) interface{} {
	ds, ok := key.(dataString)
	if ok {
		return serialization.Data(ds)
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
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#publishReservedRecord
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
	return rs.estimator.GetRecordStorageMemoryCost(rec)
}

func (rs *RecordStore) getTotalStorageMemoryCost(key interface{}, rec *Record) int64 {
	key = rs.unMakeMapKey(key)
	return rs.getKeyStorageMemoryCost(key) + rs.getRecordStorageMemoryCost(rec)

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

func (rs *RecordStore) incrementEvictions() {
	atomic.AddInt64(&rs.stats.Evictions, 1)
}

func (rs *RecordStore) getRecord(key interface{}) (*Record, bool) {
	// assumes recordsMu is locked
	value, ok := rs.records[key]
	return value, ok
}

func (rs *RecordStore) recordExpired(rec *Record, nowMS int64) bool {
	if !rs.canUpdateStats(rec) {
		// A record can only be checked for expiry if its record state is READ_PERMITTED.
		// We can't check reserved records for expiry.
		return false
	}
	if rec.IsExpiredAt(nowMS) {
		return true
	}
	return rec.IsIdleAt(rs.maxIdleMillis, nowMS)
}

func (rs *RecordStore) evict() bool {
	// port of: com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy#evict
	// see: com.hazelcast.internal.nearcache.impl.maxsize.EntryCountNearCacheEvictionChecker#isEvictionRequired
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	if len(rs.records) < rs.maxSize {
		return false
	}
	samples := rs.sample(RecordStoreSampleCount)
	c := evaluateForEviction(rs.cmp, samples)
	return rs.tryEvict(c)
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
		rec, err := rs.newReservationRecord(key, keyData, reservationID)
		if err != nil {
			return nil, err
		}
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
	rec, err := rs.newReservationRecord(key, keyData, reservationID)
	if err != nil {
		return nil, err
	}
	rs.records[key] = rec
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

func (rs *RecordStore) newReservationRecord(key interface{}, keyData serialization.Data, rid int64) (*Record, error) {
	rec, err := rs.createRecord(nil)
	if err != nil {
		return nil, err
	}
	rec.SetReservationID(rid)
	if err := rs.initInvalidationMetadata(rec, key, keyData); err != nil {
		return nil, err
	}
	return rec, nil
}

func (rs *RecordStore) initInvalidationMetadata(rec *Record, key interface{}, keyData serialization.Data) error {
	if rs.staleReadDetector == nil {
		return nil
	}
	var err error
	if keyData == nil {
		keyData, err = rs.ss.ToData(key)
		if err != nil {
			return fmt.Errorf("nearcache.RecordStore.initInvalidationMetadata: converting key: %w", err)
		}
	}
	pid, err := rs.staleReadDetector.GetPartitionID(keyData)
	if err != nil {
		return fmt.Errorf("nearcache.RecordStore.initInvalidationMetadata: getting partition ID: %w", err)
	}
	md := rs.staleReadDetector.GetMetaDataContainer(pid)
	rec.SetPartitionID(pid)
	rec.SetInvalidationSequence(md.Sequence())
	rec.SetUUID(md.UUID())
	return nil
}

func (rs *RecordStore) updateRecordValue(rec *Record, value interface{}) error {
	value, err := rs.valueConverter.ConvertValue(value)
	if err != nil {
		return err
	}
	rec.SetValue(value)
	return nil
}

func (rs *RecordStore) onExpire() {
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#onExpire
	rs.incrementExpirations()
}

func getEvictionPolicyComparator(cfg *nearcache.EvictionConfig) nearcache.EvictionPolicyComparator {
	cmp := cfg.Comparator()
	if cmp != nil {
		return cmp
	}
	switch cfg.EvictionPolicy() {
	case nearcache.EvictionPolicyLRU:
		return LRUEvictionPolicyComparator
	case nearcache.EvictionPolicyLFU:
		return LFUEvictionPolicyComparator
	case nearcache.EvictionPolicyRandom:
		return RandomEvictionPolicyComparator
	case nearcache.EvictionPolicyNone:
		return nil
	}
	msg := fmt.Sprintf("unknown eviction polcy: %d", cfg.EvictionPolicy())
	panic(ihzerrors.NewIllegalArgumentError(msg, nil))
}

type evictionCandidate struct {
	key       interface{}
	evictable *Record
}

func (e evictionCandidate) Key() interface{} {
	return e.key
}

func (e evictionCandidate) Value() interface{} {
	return e.evictable.Value()
}

func (e evictionCandidate) Hits() int64 {
	return int64(e.evictable.Hits())
}

func (e evictionCandidate) CreationTime() int64 {
	return e.evictable.CreationTime()
}

func (e evictionCandidate) LastAccessTime() int64 {
	return e.evictable.LastAccessTime()
}

func evaluateForEviction(cmp nearcache.EvictionPolicyComparator, candies []evictionCandidate) evictionCandidate {
	// see: com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator#evaluate
	now := time.Now().UnixMilli()
	var selected evictionCandidate
	var hasSelected bool
	for _, current := range candies {
		// initialize selected by setting it to current candidate.
		if !hasSelected {
			selected = current
			continue
		}
		// then check if current candidate is expired.
		if current.evictable.IsExpiredAt(now) {
			return current
		}
		// check if current candidate is more eligible than selected.
		if cmp.Compare(current, selected) < 0 {
			selected = current
			hasSelected = true
		}
	}
	return selected
}
