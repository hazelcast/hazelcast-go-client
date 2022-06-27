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
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	inearcache "github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// see: com.hazelcast.internal.nearcache.NearCache#DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS
	defaultExpirationTaskInitialDelay = 5 * time.Second
	// see: com.hazelcast.internal.nearcache.NearCache#DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS
	defaultExpirationTaskPeriod = 5 * time.Second
)

// serialization.Data is not hashable, so using this to store keys.
type dataString string

type nearCacheManager struct {
	nearCaches   map[string]*nearCache
	nearCachesMu *sync.RWMutex
	ss           *serialization.Service
	rt           *nearCacheReparingTask
	lg           ilogger.LogAdaptor
	doneCh       chan struct{}
	state        int32
}

func newNearCacheManager(ic *client.Client) *nearCacheManager {
	doneCh := make(chan struct{})
	cs := ic.ClusterService
	is := ic.InvocationService
	ss := ic.SerializationService
	ps := ic.PartitionService
	inf := ic.InvocationFactory
	lg := ic.Logger
	mf := newNearCacheInvalidationMetaDataFetcher(cs, is, inf, lg)
	reconciliationIntervalSeconds := 60
	maxToleratedMissCount := 10
	rt := newNearCacheReparingTask(reconciliationIntervalSeconds, maxToleratedMissCount, ss, ps, lg, mf, doneCh)
	ncm := &nearCacheManager{
		nearCaches:   map[string]*nearCache{},
		nearCachesMu: &sync.RWMutex{},
		ss:           ss,
		rt:           rt,
		lg:           lg,
		doneCh:       doneCh,
	}
	return ncm
}

func (m *nearCacheManager) Stop() {
	if atomic.CompareAndSwapInt32(&m.state, 0, 1) {
		close(m.doneCh)
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
		nc = newNearCache(&cfg, m.ss, m.lg, m.doneCh)
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
	store                nearCacheRecordStore
	cfg                  *nearcache.Config
	logger               ilogger.LogAdaptor
	doneCh               <-chan struct{}
	expirationInProgress int32
}

func newNearCache(cfg *nearcache.Config, ss *serialization.Service, logger ilogger.LogAdaptor, doneCh <-chan struct{}) *nearCache {
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
	nc := &nearCache{
		cfg:    cfg,
		store:  newNearCacheRecordStore(cfg, ss, rc, se),
		logger: logger,
		doneCh: doneCh,
	}
	go nc.startExpirationTask(defaultExpirationTaskInitialDelay, defaultExpirationTaskPeriod)
	return nc
}

func (nc *nearCache) Clear() {
	nc.store.Clear()
}

func (nc *nearCache) Get(key interface{}) (interface{}, bool, error) {
	_, ok := key.(serialization.Data)
	if nc.cfg.SerializeKeys {
		if !ok {
			panic("key must be of type serialization.Data!")
		}
	} else if ok {
		panic("key cannot be of type Data!")
	}
	return nc.store.Get(key)
}

func (nc *nearCache) Invalidate(key interface{}) {
	nc.store.Invalidate(key)
}

func (nc nearCache) Size() int {
	return nc.store.Size()
}

func (nc nearCache) Stats() nearcache.Stats {
	return nc.store.Stats()
}

func (nc nearCache) InvalidationRequests() int64 {
	return nc.store.InvalidationRequests()
}

func (nc *nearCache) TryReserveForUpdate(key interface{}, keyData serialization.Data, ups nearCacheUpdateSemantic) (int64, error) {
	// port of: com.hazelcast.internal.nearcache.impl.DefaultNearCache#tryReserveForUpdate
	nc.store.doEviction()
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

func (nc *nearCache) startExpirationTask(delay, timeout time.Duration) {
	time.Sleep(delay)
	timer := time.NewTicker(timeout)
	defer timer.Stop()
	for {
		select {
		case <-nc.doneCh:
			return
		case <-timer.C:
			if atomic.CompareAndSwapInt32(&nc.expirationInProgress, 0, 1) {
				nc.logger.Debug(func() string {
					return "running near cache expiration task"
				})
				nc.store.DoExpiration()
				atomic.StoreInt32(&nc.expirationInProgress, 0)
			}
		}
	}
}

const (
	nearCacheRecordStoreTimeNotSet int64 = -1
	// see: com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy#SAMPLE_COUNT
	nearCacheRecordStoreSampleCount       = 15
	pointerCostInBytes                    = (32 << uintptr(^uintptr(0)>>63)) >> 3
	int32CostInBytes                      = 4
	int64CostInBytes                      = 8
	atomicValueCostInBytes                = 8
	uuidCostInBytes                       = 16 // low uint64 + high uint64
	nearCacheRecordNotReserved      int64 = -1
	nearCacheRecordReadPermitted          = -2
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
	if rec == nil {
		return 0
	}
	cost := pointerCostInBytes + // the record is stored as a pointer in the map
		5*int64CostInBytes + // CreationTime, lastAccessTime, ExpirationTime, InvalidationSequence, reservationID
		3*int32CostInBytes + // PartitionID, hits, cachedAsNil
		1*atomicValueCostInBytes + // holder of the value
		1*uuidCostInBytes // UUID
	value := rec.Value()
	data, ok := value.(serialization.Data)
	if ok {
		cost += data.DataSize()
	}
	return int64(cost)
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
	stats            nearCacheStats
	maxIdleMillis    int64
	reservationID    int64
	timeToLiveMillis int64
	recordsMu        *sync.Mutex
	records          map[interface{}]*nearCacheRecord
	ss               *serialization.Service
	valueConverter   nearCacheRecordValueConverter
	estimator        nearCacheStorageEstimator
	evictionDisabled bool
	maxSize          int
	cmp              nearcache.EvictionPolicyComparator
}

func newNearCacheRecordStore(cfg *nearcache.Config, ss *serialization.Service, rc nearCacheRecordValueConverter, se nearCacheStorageEstimator) nearCacheRecordStore {
	stats := nearCacheStats{
		CreationTime: time.Now(),
	}
	return nearCacheRecordStore{
		recordsMu:        &sync.Mutex{},
		records:          map[interface{}]*nearCacheRecord{},
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

func (rs *nearCacheRecordStore) Clear() {
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

func (rs *nearCacheRecordStore) Get(key interface{}) (value interface{}, found bool, err error) {
	// checkAvailable() does not apply since rs.records is always created
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	key = rs.makeMapKey(key)
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
		rs.invalidate(key)
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

func (rs *nearCacheRecordStore) Invalidate(key interface{}) {
	rs.recordsMu.Lock()
	rs.invalidate(key)
	rs.recordsMu.Unlock()
}

func (rs *nearCacheRecordStore) invalidate(key interface{}) {
	// assumes rs.recordsMu is locked.
	key = rs.makeMapKey(key)
	var canUpdateStats bool
	rec, exists := rs.records[key]
	if exists {
		delete(rs.records, key)
		canUpdateStats = rec.ReservationID() == nearCacheRecordReadPermitted
	}
	if canUpdateStats {
		rs.decrementOwnedEntryCount()
		rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
		rs.incrementInvalidations()
	}
	rs.incrementInvalidationRequests()
}

func (rs *nearCacheRecordStore) TryPublishReserved(key interface{}, value interface{}, reservationID int64, deserialize bool) (interface{}, error) {
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

func (rs *nearCacheRecordStore) doEviction() bool {
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#doEviction
	// note that the reference implementation never has withoutMaxSizeCheck == true
	// checkAvailable doesn't apply
	if rs.evictionDisabled {
		return false
	}
	return rs.evict()
}

func (rs *nearCacheRecordStore) DoExpiration() {
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

func (rs *nearCacheRecordStore) onEvict(key interface{}, rec *nearCacheRecord, expired bool) {
	// port of: com.hazelcast.internal.nearcache.impl.store.BaseHeapNearCacheRecordStore#onEvict
	if expired {
		rs.incrementExpirations()
	} else {
		rs.incrementEvictions()
	}
	rs.decrementOwnedEntryCount()
	rs.decrementOwnedEntryMemoryCost(rs.getTotalStorageMemoryCost(key, rec))
}

func (rs *nearCacheRecordStore) tryEvict(candidate evictionCandidate) bool {
	// port of: com.hazelcast.internal.nearcache.impl.store.HeapNearCacheRecordMap#tryEvict
	if exists := rs.remove(candidate.Key()); !exists {
		return false
	}
	rs.onEvict(candidate.key, candidate.evictable, false)
	return true
}

func (rs *nearCacheRecordStore) remove(key interface{}) bool {
	// the key is already in the "made" form, so don't run on rs.makeMapKey on it
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	if _, exists := rs.records[key]; !exists {
		return false
	}
	delete(rs.records, key)
	return true
}

func (rs *nearCacheRecordStore) sample(count int) []evictionCandidate {
	// port of: com.hazelcast.internal.nearcache.impl.store.HeapNearCacheRecordMap#sample
	// note that count is fixed to 15 in the reference implementation, it is always positive
	// assumes recordsMu is locked
	samples := make([]evictionCandidate, count)
	var idx int
	for k, v := range rs.records {
		// access to maps is random
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

func (rs nearCacheRecordStore) Stats() nearcache.Stats {
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

func (rs nearCacheRecordStore) InvalidationRequests() int64 {
	return atomic.LoadInt64(&rs.stats.invalidationRequests)
}

func (rs nearCacheRecordStore) Size() int {
	rs.recordsMu.Lock()
	size := len(rs.records)
	rs.recordsMu.Unlock()
	return size
}

func (rs nearCacheRecordStore) makeMapKey(key interface{}) interface{} {
	data, ok := key.(serialization.Data)
	if ok {
		// serialization.Data is not hashable, convert it to string
		return dataString(data)
	}
	return key
}

func (rs nearCacheRecordStore) unMakeMapKey(key interface{}) interface{} {
	ds, ok := key.(dataString)
	if ok {
		return serialization.Data(ds)
	}
	return key
}

func (rs *nearCacheRecordStore) toValue(v interface{}) (interface{}, error) {
	data, ok := v.(serialization.Data)
	if !ok {
		return v, nil
	}
	return rs.ss.ToObject(data)
}

func (rs *nearCacheRecordStore) publishReservedRecord(key, value interface{}, rec *nearCacheRecord, reservationID int64) (*nearCacheRecord, error) {
	// port of: com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#publishReservedRecord
	if rec.ReservationID() != reservationID {
		return rec, nil
	}
	cost := rs.getTotalStorageMemoryCost(key, rec)
	update := rec.Value() != nil || rec.CachedAsNil()
	if update {
		rs.decrementOwnedEntryMemoryCost(cost)
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
	key = rs.unMakeMapKey(key)
	return rs.getKeyStorageMemoryCost(key) + rs.getRecordStorageMemoryCost(rec)
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
	atomic.AddInt64(&rs.stats.invalidationRequests, 1)
}

func (rs *nearCacheRecordStore) incrementEvictions() {
	atomic.AddInt64(&rs.stats.Evictions, 1)
}

func (rs *nearCacheRecordStore) getRecord(key interface{}) (*nearCacheRecord, bool) {
	// assumes recordsMu is locked
	value, ok := rs.records[key]
	return value, ok
}

func (rs *nearCacheRecordStore) recordExpired(rec *nearCacheRecord, nowMS int64) bool {
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

func (rs *nearCacheRecordStore) canUpdateStats(rec *nearCacheRecord) bool {
	return rec != nil && rec.ReservationID() == nearCacheRecordReadPermitted
}

func (rs *nearCacheRecordStore) evict() bool {
	// port of: com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy#evict
	// see: com.hazelcast.internal.nearcache.impl.maxsize.EntryCountNearCacheEvictionChecker#isEvictionRequired
	rs.recordsMu.Lock()
	defer rs.recordsMu.Unlock()
	if len(rs.records) < rs.maxSize {
		return false
	}
	samples := rs.sample(nearCacheRecordStoreSampleCount)
	c := evaluateForEviction(rs.cmp, samples)
	return rs.tryEvict(c)
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

func (rs *nearCacheRecordStore) createRecord(value interface{}) (*nearCacheRecord, error) {
	// assumes recordsMu was locked elsewhere
	var err error
	value, err = rs.valueConverter.ConvertValue(value)
	if err != nil {
		return nil, err
	}
	created := time.Now().UnixMilli()
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

func (rs *nearCacheRecordStore) onExpire() {
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
		return inearcache.LRUEvictionPolicyComparator
	case nearcache.EvictionPolicyLFU:
		return inearcache.LFUEvictionPolicyComparator
	case nearcache.EvictionPolicyRandom:
		return inearcache.RandomEvictionPolicyComparator
	case nearcache.EvictionPolicyNone:
		return nil
	}
	msg := fmt.Sprintf("unknown eviction polcy: %d", cfg.EvictionPolicy())
	panic(ihzerrors.NewIllegalArgumentError(msg, nil))
}

type nearCacheRecord struct {
	value                internal.AtomicValue
	UUID                 types.UUID
	InvalidationSequence int64
	PartitionID          int32
	reservationID        int64
	creationTime         int32
	lastAccessTime       int32
	expirationTime       int32
	hits                 int32
	cachedAsNil          int32
}

func newNearCacheRecord(value interface{}, creationTime, expirationTime int64) *nearCacheRecord {
	av := internal.AtomicValue{}
	av.Store(&value)
	rec := &nearCacheRecord{value: av}
	rec.SetCreationTime(creationTime)
	rec.SetExpirationTIme(expirationTime)
	return rec
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

func (r *nearCacheRecord) CreationTime() int64 {
	t := atomic.LoadInt32(&r.creationTime)
	return inearcache.RecomputeWithBaseTime(t)
}

func (r *nearCacheRecord) SetCreationTime(ms int64) {
	secs := inearcache.StripBaseTime(ms)
	atomic.StoreInt32(&r.creationTime, secs)
}

func (r *nearCacheRecord) LastAccessTime() int64 {
	t := atomic.LoadInt32(&r.lastAccessTime)
	return inearcache.RecomputeWithBaseTime(t)
}

func (r *nearCacheRecord) SetLastAccessTime(ms int64) {
	secs := inearcache.StripBaseTime(ms)
	atomic.StoreInt32(&r.lastAccessTime, secs)
}

func (r *nearCacheRecord) ExpirationTime() int64 {
	t := atomic.LoadInt32(&r.expirationTime)
	return inearcache.RecomputeWithBaseTime(t)
}

func (r *nearCacheRecord) SetExpirationTIme(ms int64) {
	secs := inearcache.StripBaseTime(ms)
	atomic.StoreInt32(&r.expirationTime, secs)
}

func (r *nearCacheRecord) IsExpiredAt(ms int64) bool {
	t := r.ExpirationTime()
	return t > 0 && t <= ms
}

func (r *nearCacheRecord) IsIdleAt(maxIdleMS, nowMS int64) bool {
	if maxIdleMS <= 0 {
		return false
	}
	lat := r.LastAccessTime()
	if lat > 0 {
		return lat+maxIdleMS < nowMS
	}
	return r.CreationTime()+maxIdleMS < nowMS
}

func (r *nearCacheRecord) CachedAsNil() bool {
	return atomic.LoadInt32(&r.cachedAsNil) == 1
}

func (r *nearCacheRecord) SetCachedAsNil() {
	atomic.StoreInt32(&r.cachedAsNil, 1)
}

type evictionCandidate struct {
	key       interface{}
	evictable *nearCacheRecord
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

// nearCacheStats contains statistics for a Near Cache instance.
type nearCacheStats struct {
	invalidationRequests int64
	// Misses is the number of times a key was not found in the Near Cache.
	Misses int64
	// Hits is the number of times a key was found in the Near Cache.
	Hits int64
	// Expirations is the number of expirations.
	Expirations int64
	// Evictions is the number of evictions.
	Evictions int64
	// OwnedEntryCount is the number of entries in the Near Cache.
	OwnedEntryCount int64
	// OwnedEntryMemoryCost is the estimated memory cost of the entries in the Near Cache.
	OwnedEntryMemoryCost int64
	// Invalidations is the number of successful invalidations.
	Invalidations int64
	// LastPersistenceKeyCount is the number of keys saved in the last persistence task.
	LastPersistenceKeyCount int64
	// LastPersistenceWrittenBytes is the size of the last persistence task.
	LastPersistenceWrittenBytes int64
	// PersistenceCount is the number of completed persistence tasks.
	PersistenceCount int64
	// CreationTime is the time the Near Cache was initialized.
	CreationTime time.Time
	// LastPersistenceTime is the time of the last completed persistence task.
	LastPersistenceTime time.Time
	// LastPersistenceFailure is the error message of the last completed persistence task.
	LastPersistenceFailure string
	// LastPersistenceDuration is the duration of the last completed persistence task.
	LastPersistenceDuration time.Duration
}

func (st *nearCacheStats) InvalidationRequests() int64 {
	return atomic.LoadInt64(&st.invalidationRequests)
}

/*
nearCacheReparingTask runs on Near Cache side and only one instance is created per data-structure type like IMap and ICache.
Repairing responsibilities of this task are:

    * To scan RepairingHandlers to see if any Near Cache needs to be invalidated according to missed invalidation counts (controlled via InvalidationMaxToleratedMissCount).
    * To send periodic generic-operations to cluster members in order to fetch latest partition sequences and UUIDs (controlled via InvalidationMinReconciliationIntervalSeconds.

See: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask
*/
type nearCacheReparingTask struct {
	handlers                    *sync.Map
	ss                          *serialization.Service
	ps                          *cluster.PartitionService
	doneCh                      <-chan struct{}
	invalidationMetaDataFetcher nearCacheInvalidationMetaDataFetcher
	lg                          ilogger.LogAdaptor
	reconciliationIntervalNanos int64
	lastAntiEntropyRunNanos     int64
	maxToleratedMissCount       int
	partitionCount              int32
}

func newNearCacheReparingTask(recInt int, maxMissCnt int, ss *serialization.Service, ps *cluster.PartitionService, lg ilogger.LogAdaptor, mf nearCacheInvalidationMetaDataFetcher, doneCh <-chan struct{}) *nearCacheReparingTask {
	nc := &nearCacheReparingTask{
		reconciliationIntervalNanos: int64(recInt) * 1_000_000_000,
		maxToleratedMissCount:       maxMissCnt,
		invalidationMetaDataFetcher: mf,
		doneCh:                      doneCh,
		ss:                          ss,
		ps:                          ps,
		lg:                          lg,
		handlers:                    &sync.Map{},
		partitionCount:              ps.PartitionCount(),
	}
	go nc.Start()
	return nc
}

func (rt *nearCacheReparingTask) Start() {
	// see: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#run
	rt.lg.Debug(func() string {
		return "nearCacheReparingTask started"
	})
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()
	rt.doTask()
	for {
		select {
		case <-rt.doneCh:
			return
		case <-timer.C:
			rt.doTask()
		}
	}
}

func (rt *nearCacheReparingTask) doTask() {
	rt.fixSequenceGaps()
	if rt.isAntiEntropyNeeded() {
		if err := rt.runAntiEntropy(context.Background()); err != nil {
			rt.lg.Errorf("%s", err.Error())
		}
	}
}

// fixSequenceGaps marks relevant data as stale if missed invalidation event count is above the max tolerated miss count.
func (rt *nearCacheReparingTask) fixSequenceGaps() {
	rt.handlers.Range(func(_, value interface{}) bool {
		handler := value.(nearCacheRepairingHandler)
		if rt.isAboveMaxToleratedMissCount(handler) {
			rt.updateLastKnownStaleSequences(handler)
		}
		return true
	})
}

func (rt *nearCacheReparingTask) isAboveMaxToleratedMissCount(handler nearCacheRepairingHandler) bool {
	var total int64
	for i := int32(0); i < rt.partitionCount; i++ {
		md := handler.GetMetaDataContainer(i)
		total += md.MissedSequenceCount()
		if total > int64(rt.maxToleratedMissCount) {
			rt.lg.Trace(func() string {
				return fmt.Sprintf("above tolerated miss count:[map=%s,missCount=%d,maxToleratedMissCount=%d]",
					handler.Name(), total, rt.maxToleratedMissCount)
			})
			return true
		}
	}
	return false
}

func (rt *nearCacheReparingTask) updateLastKnownStaleSequences(handler nearCacheRepairingHandler) {
	for i := int32(0); i < rt.partitionCount; i++ {
		md := handler.GetMetaDataContainer(i)
		mc := md.MissedSequenceCount()
		if mc != 0 {
			// return value is ignored.
			md.AddAndGetMissedSequenceCount(-mc)
			handler.UpdateLastKnownStaleSequence(md, i)
		}
	}
}

func (rt *nearCacheReparingTask) isAntiEntropyNeeded() bool {
	if rt.reconciliationIntervalNanos == 0 {
		return false
	}
	now := time.Now()
	since := int64(now.Nanosecond()) - atomic.LoadInt64(&rt.lastAntiEntropyRunNanos)
	return since > rt.reconciliationIntervalNanos
}

// runAntiEntropy periodically sends generic operations to cluster members to get latest invalidation metadata.
func (rt *nearCacheReparingTask) runAntiEntropy(ctx context.Context) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#runAntiEntropy
	rt.lg.Debug(func() string {
		return "nearCacheReparingTask.runAntiEntropy"
	})
	// get a copy of the handlers, so we don't have to deal with sync.Map.
	handlers := map[string]nearCacheRepairingHandler{}
	rt.handlers.Range(func(k, v interface{}) bool {
		handlers[k.(string)] = v.(nearCacheRepairingHandler)
		return true
	})
	err := rt.invalidationMetaDataFetcher.fetchMetadata(ctx, handlers)
	if err != nil {
		return fmt.Errorf("nearCacheReparingTask: runAntiEntropy: %w", err)
	}
	return nil
}

// nearCacheRepairingHandler is the port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler
type nearCacheRepairingHandler struct {
	name               string
	metadataContainers []*nearCacheMetaDataContainer
	nc                 *nearCache
	toNearCacheKey     func(key interface{}) (interface{}, error)
	ss                 *serialization.Service
	ps                 *cluster.PartitionService
	lg                 ilogger.LogAdaptor
}

func newNearCacheRepairingHandler(name string, nc *nearCache, ncc *nearcache.Config, partitionCount int, ss *serialization.Service, ps *cluster.PartitionService, lg ilogger.LogAdaptor) *nearCacheRepairingHandler {
	mcs := make([]*nearCacheMetaDataContainer, partitionCount)
	for i := 0; i < partitionCount; i++ {
		mcs[i] = newNearCacheMetaDataContainer()
	}
	sk := ncc.SerializeKeys
	f := func(key interface{}) (interface{}, error) {
		if sk {
			return key, nil
		}
		return ss.ToObject(key.(serialization.Data))
	}
	return &nearCacheRepairingHandler{
		name:               name,
		nc:                 nc,
		metadataContainers: mcs,
		toNearCacheKey:     f,
		ps:                 ps,
		lg:                 lg,
	}
}

func (h nearCacheRepairingHandler) Name() string {
	return h.name
}

func (h nearCacheRepairingHandler) GetMetaDataContainer(partition int32) *nearCacheMetaDataContainer {
	return h.metadataContainers[partition]
}

func (h *nearCacheRepairingHandler) UpdateLastKnownStaleSequence(md *nearCacheMetaDataContainer, partition int32) {
	var lastReceived int64
	var lastKnown int64
	for {
		lastReceived = md.Sequence()
		lastKnown = md.StaleSequence()
		if lastKnown >= lastReceived {
			break
		}
		if md.CASStaleSequence(lastKnown, lastReceived) {
			break
		}
	}
	h.lg.Trace(func() string {
		return fmt.Sprintf("stale sequences updated:[map=%s,partition=%d,lowerSequencesStaleThan=%d,lastReceivedSequence=%d]",
			h.name, partition, md.StaleSequence(), md.Sequence())
	})
}

// handle handles a single invalidation
func (h *nearCacheRepairingHandler) handle(key serialization.Data, source, partition types.UUID, seq int64) error {
	// Apply invalidation if it's not originated by local member/client.
	// Since local Near Caches are invalidated immediately there is no need to invalidate them twice.
	if source != partition {
		if key == nil {
			h.nc.Clear()
		} else {
			k, err := h.toNearCacheKey(key)
			if err != nil {
				return err
			}
			h.nc.Invalidate(k)
		}
	}
	pid, err := h.getPartitionIDOrDefault(key)
	if err != nil {
		return err
	}
	h.CheckOrRepairUUID(pid, partition)
	h.CheckOrRepairSequence(pid, seq, false)
	return nil
}

func (h *nearCacheRepairingHandler) CheckOrRepairUUID(partition int32, new types.UUID) {
	// this method may be called concurrently: anti-entropy, event service
	if new.Default() {
		panic("CheckOrRepairUUID: new UUID should not be the default UUID")
	}
	md := h.GetMetaDataContainer(partition)
	for {
		prev := md.UUID()
		if prev == new {
			break
		}
		if md.CASUUID(prev, new) {
			md.ResetStaleSequence()
			md.ResetStaleSequence()
			h.lg.Trace(func() string {
				return fmt.Sprintf("invalid UUID, lost remote partition data unexpectedly:[name=%s,partition=%d,prevUuid=%s,newUuid=%s]",
					h.name, partition, prev, new)
			})
			break
		}
	}
}

func (h *nearCacheRepairingHandler) CheckOrRepairSequence(partition int32, nextSeq int64, viaAntiEntropy bool) {
	if nextSeq <= 0 {
		panic("CheckOrRepairSequence <= 0")
	}
	md := h.GetMetaDataContainer(partition)
	for {
		curSeq := md.Sequence()
		if curSeq >= nextSeq {
			break
		}
		if md.CASSequence(curSeq, nextSeq) {
			diff := nextSeq - curSeq
			if viaAntiEntropy || diff > 1 {
				// We have found at least one missing sequence between current and next sequences.
				// If miss is detected by anti-entropy, number of missed sequences will be miss = next - current.
				// Otherwise it means miss is detected by observing received invalidation event sequence numbers and number of missed sequences will be miss = next - current - 1.
				missCnt := diff
				if !viaAntiEntropy {
					missCnt -= 1
				}
				total := md.AddAndGetMissedSequenceCount(missCnt)
				h.lg.Trace(func() string {
					return fmt.Sprintf("invalid sequence:[map=%s,partition=%d,currentSequence=%d,nextSequence=%d,totalMissCount=%d]",
						h.name, partition, curSeq, nextSeq, total)
				})
			}
			break
		}
	}
}

func (h nearCacheRepairingHandler) getPartitionIDOrDefault(key serialization.Data) (int32, error) {
	if key == nil {
		// name is used to determine partition ID of map-wide events like clear()
		// since key is nil, we are using name to find the partition ID
		data, err := h.ss.ToData(h.name)
		if err != nil {
			return 0, err
		}
		key = data
	}
	return h.ps.GetPartitionID(key)
}

// nearCacheMetaDataContainer contains one partitions' invalidation metadata.
// port of: com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer
type nearCacheMetaDataContainer struct {
	seq        int64
	staleSeq   int64
	missedSeqs int64
	uuid       atomic.Value
}

func newNearCacheMetaDataContainer() *nearCacheMetaDataContainer {
	uuid := atomic.Value{}
	uuid.Store(types.UUID{})
	return &nearCacheMetaDataContainer{
		uuid: uuid,
	}
}

func (mc *nearCacheMetaDataContainer) SetUUID(uuid types.UUID) {
	mc.uuid.Store(uuid)
}

func (mc nearCacheMetaDataContainer) UUID() types.UUID {
	return mc.uuid.Load().(types.UUID)
}

func (mc *nearCacheMetaDataContainer) CASUUID(prev, new types.UUID) bool {
	return mc.uuid.CompareAndSwap(prev, new)
}

func (mc *nearCacheMetaDataContainer) SetSequence(seq int64) {
	atomic.StoreInt64(&mc.seq, seq)
}

func (mc nearCacheMetaDataContainer) Sequence() int64 {
	return atomic.LoadInt64(&mc.seq)
}

func (mc *nearCacheMetaDataContainer) ResetSequence() {
	mc.SetSequence(0)
}

func (mc *nearCacheMetaDataContainer) CASSequence(current, next int64) bool {
	return atomic.CompareAndSwapInt64(&mc.seq, current, next)
}

func (mc *nearCacheMetaDataContainer) SetStaleSequence(seq int64) {
	atomic.StoreInt64(&mc.staleSeq, seq)
}

func (mc nearCacheMetaDataContainer) StaleSequence() int64 {
	return atomic.LoadInt64(&mc.staleSeq)
}

func (mc *nearCacheMetaDataContainer) ResetStaleSequence() {
	mc.SetStaleSequence(0)
}

func (mc *nearCacheMetaDataContainer) CASStaleSequence(lastKnown, lastReceived int64) bool {
	return atomic.CompareAndSwapInt64(&mc.staleSeq, lastKnown, lastReceived)
}

func (mc *nearCacheMetaDataContainer) SetMissedSequenceCount(count int64) {
	atomic.StoreInt64(&mc.missedSeqs, count)
}

func (mc nearCacheMetaDataContainer) MissedSequenceCount() int64 {
	return atomic.LoadInt64(&mc.missedSeqs)
}

func (mc *nearCacheMetaDataContainer) AddAndGetMissedSequenceCount(count int64) int64 {
	return atomic.AddInt64(&mc.missedSeqs, count)
}

// nearCacheInvalidationMetaDataFetcher runs on Near Cache side.
// An instance of this task is responsible for fetching of all Near Caches' remote metadata like last sequence numbers and partition UUIDs.
// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher
// port of: com.hazelcast.client.map.impl.nearcache.invalidation.ClientMapInvalidationMetaDataFetcher
type nearCacheInvalidationMetaDataFetcher struct {
	cs         *cluster.Service
	is         *invocation.Service
	invFactory *cluster.ConnectionInvocationFactory
	lg         ilogger.LogAdaptor
}

func newNearCacheInvalidationMetaDataFetcher(cs *cluster.Service, is *invocation.Service, invFactory *cluster.ConnectionInvocationFactory, lg ilogger.LogAdaptor) nearCacheInvalidationMetaDataFetcher {
	df := nearCacheInvalidationMetaDataFetcher{
		cs:         cs,
		is:         is,
		invFactory: invFactory,
		lg:         lg,
	}
	return df
}

func (df nearCacheInvalidationMetaDataFetcher) fetchMetadata(ctx context.Context, handlers map[string]nearCacheRepairingHandler) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#fetchMetadata
	if len(handlers) == 0 {
		return nil
	}
	// getDataStructureNames
	names := []string{}
	for _, h := range handlers {
		names = append(names, h.Name())
	}
	invs, err := df.fetchMembersMetadataFor(ctx, names)
	if err != nil {
		return fmt.Errorf("nearCacheInvalidationMetaDataFetcher.fetchMetadata: fetching members metadata: %w", err)
	}
	for _, inv := range invs {
		if err := df.processMemberMetadata(ctx, inv, handlers); err != nil {
			return fmt.Errorf("nearCacheInvalidationMetaDataFetcher.fetchMetadata: processing metadata: %w", err)
		}
	}
	return nil
}

func (df nearCacheInvalidationMetaDataFetcher) fetchMembersMetadataFor(ctx context.Context, names []string) (map[types.UUID]invocation.Invocation, error) {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#fetchMembersMetadataFor
	mems := filterDataMembers(df.cs.OrderedMembers())
	if len(mems) == 0 {
		return nil, nil
	}
	invs := make(map[types.UUID]invocation.Invocation, len(mems))
	for _, mem := range mems {
		inv, err := df.fetchMetaDataOf(ctx, mem, names)
		if err != nil {
			return nil, err
		}
		invs[mem.UUID] = inv
	}
	return invs, nil
}

func (df nearCacheInvalidationMetaDataFetcher) fetchMetaDataOf(ctx context.Context, mem pubcluster.MemberInfo, names []string) (*cluster.MemberBoundInvocation, error) {
	// port of: com.hazelcast.client.map.impl.nearcache.invalidation.ClientMapInvalidationMetaDataFetcher#fetchMetadataOf
	msg := codec.EncodeMapFetchNearCacheInvalidationMetadataRequest(names, mem.UUID)
	inv := df.invFactory.NewMemberBoundInvocation(msg, &mem, time.Now())
	if err := df.is.SendRequest(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

func (df *nearCacheInvalidationMetaDataFetcher) processMemberMetadata(ctx context.Context, inv invocation.Invocation, handlers map[string]nearCacheRepairingHandler) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#processMemberMetadata
	// extractMemberMetadata
	res, err := inv.GetWithContext(ctx)
	if err != nil {
		return err
	}
	npsPairs, psPairs := codec.DecodeMapFetchNearCacheInvalidationMetadataResponse(res)
	// repairUuids
	// see: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#repairUuids
	for _, p := range psPairs {
		k := p.Key.(int32)
		v := p.Value.(types.UUID)
		for _, handler := range handlers {
			handler.CheckOrRepairUUID(k, v)
		}
	}
	// repairSequences
	// see: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#repairSequences
	for _, np := range npsPairs {
		handler := handlers[np.Key.(string)]
		vvs := np.Value.([]proto.Pair)
		for _, vv := range vvs {
			k := vv.Key.(int32)
			v := vv.Value.(int64)
			handler.CheckOrRepairSequence(k, v, true)
		}
	}
	return nil
}

// filterDataMembers removes lite members from the given slice.
// Order of the members is not preserved.
func filterDataMembers(mems []pubcluster.MemberInfo) []pubcluster.MemberInfo {
	di := len(mems)
loop:
	for i := 0; i < di; i++ {
		if mems[i].LiteMember {
			// order is not important, delete by moving deleted items to the end.
			// find the first non-lite member
			di--
			for mems[di].LiteMember {
				if di <= i {
					// no more data members left
					break loop
				}
				di--
			}
			mems[i], mems[di] = mems[di], mems[i]
		}
	}
	// all deleted items are at the end.
	// shrink the slice to get rid of them.
	return mems[:di]
}
