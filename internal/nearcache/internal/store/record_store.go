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

package store

import (
	"time"

	"sort"

	"sync/atomic"

	"sync"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record/comparator"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

const evictionPercentage = 20

type recordsHolder struct {
	records    []nearcache.Record
	comparator nearcache.RecordComparator
}

func (r recordsHolder) Len() int {
	return len(r.records)
}

func (r recordsHolder) Less(i, j int) bool {
	return r.records[i].LessThan(r.comparator, r.records[j])
}

func (r recordsHolder) Swap(i, j int) {
	r.records[i], r.records[j] = r.records[j], r.records[i]
}

type NearCacheRecordStore struct {
	recordsMu            sync.RWMutex
	records              map[interface{}]nearcache.Record
	config               *config.NearCacheConfig
	serializationService spi.SerializationService
	maxIdleDuration      time.Duration
	timeToLiveDuration   time.Duration
	staleReadDetector    nearcache.StaleReadDetector
	evictionDisabled     bool
	evictionPolicy       config.EvictionPolicy
	maxSize              int32
	recordComparator     nearcache.RecordComparator
	reservationID        int64
}

func New(nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService) *NearCacheRecordStore {
	a := &NearCacheRecordStore{
		records:              make(map[interface{}]nearcache.Record),
		config:               nearCacheCfg,
		serializationService: service,
		staleReadDetector:    nearcache.AlwaysFresh,
		maxSize:              nearCacheCfg.MaxEntryCount(),
		evictionPolicy:       nearCacheCfg.EvictionPolicy(),
		evictionDisabled:     nearCacheCfg.EvictionPolicy() == config.EvictionPolicyNone,
		maxIdleDuration:      nearCacheCfg.MaxIdleDuration(),
		timeToLiveDuration:   nearCacheCfg.TimeToLive(),
	}

	a.initRecordComparator()
	return a
}

func (a *NearCacheRecordStore) initRecordComparator() {
	if a.evictionPolicy == config.EvictionPolicyLru {
		a.recordComparator = &comparator.LRUComparator{}
	}
	if a.evictionPolicy == config.EvictionPolicyLfu {
		a.recordComparator = &comparator.LFUComparator{}
	}
}

func (a *NearCacheRecordStore) nextReservationID() int64 {
	return atomic.AddInt64(&a.reservationID, 1)
}

func (a *NearCacheRecordStore) Get(key interface{}) interface{} {
	if record, found := a.Record(key); found {
		if record.RecordState() != nearcache.ReadPermitted {
			return nil
		}
		if a.staleReadDetector.IsStaleRead(key, record) {
			a.Invalidate(key)
			return nil
		}
		if a.isRecordExpired(record) {
			a.Invalidate(key)
			return nil
		}
		a.onRecordAccess(record)
		value := a.recordToValue(record)
		return value
	}
	return nil
}

func (a *NearCacheRecordStore) Put(key interface{}, value interface{}) {
	if a.evictionPolicy == config.EvictionPolicyNone && a.Size() >= int(a.maxSize) && !a.containsKey(key) {
		return
	}

	keyData := a.toData(key)
	record := a.createRecordFromValue(key, value)
	a.onRecordCreate(key, keyData, record)
	a.putRecord(key, record)
}

func (a *NearCacheRecordStore) containsKey(key interface{}) bool {
	a.recordsMu.RLock()
	defer a.recordsMu.RUnlock()
	_, found := a.records[a.nearcacheKey(key)]
	return found
}

func (a *NearCacheRecordStore) SetStaleReadDetector(detector nearcache.StaleReadDetector) {
	a.staleReadDetector = detector
}

func (a *NearCacheRecordStore) TryReserveForUpdate(key interface{},
	keyData serialization.Data) (reservationID int64, reserved bool) {
	if a.evictionPolicy == config.EvictionPolicyNone && a.Size() >= int(a.maxSize) && !a.containsKey(key) {
		return 0, false
	}
	reservedRecord := a.getOrCreateToReserve(key)
	reservationID = a.nextReservationID()
	if reservedRecord.CasRecordState(nearcache.Reserved, reservationID) {
		return reservationID, true
	}
	return 0, false
}

func (a *NearCacheRecordStore) getOrCreateToReserve(key interface{}) nearcache.Record {
	a.recordsMu.RLock()
	if record, found := a.records[a.nearcacheKey(key)]; found {
		defer a.recordsMu.RUnlock()
		return record
	}
	a.recordsMu.RUnlock()
	keyData, _ := a.serializationService.ToData(key)
	record := a.createRecordFromValue(key, nil)
	a.onRecordCreate(key, keyData, record)
	record.CasRecordState(nearcache.ReadPermitted, nearcache.Reserved)
	a.putRecord(key, record)
	return record
}

func (a *NearCacheRecordStore) TryPublishReserved(key interface{},
	value interface{}, reservationID int64, deserialize bool) (interface{}, bool) {
	a.recordsMu.RLock()
	reservedRecord, found := a.records[a.nearcacheKey(key)]
	a.recordsMu.RUnlock()
	if !found {
		return nil, false
	}
	if !reservedRecord.CasRecordState(reservationID, nearcache.UpdateStarted) {
		return nil, false
	}
	a.updateRecordValue(reservedRecord, value)
	reservedRecord.CasRecordState(nearcache.UpdateStarted, nearcache.ReadPermitted)
	cachedValue := reservedRecord.Value()
	return a.toValue(cachedValue), true
}

func (a *NearCacheRecordStore) updateRecordValue(record nearcache.Record, value interface{}) {
	if a.config.InMemoryFormat() == config.InMemoryFormatObject {
		record.SetValue(a.toValue(value))
	} else {
		record.SetValue(a.toData(value))
	}
}

func (a *NearCacheRecordStore) delete(key interface{}) {
	delete(a.records, a.nearcacheKey(key))
}

func (a *NearCacheRecordStore) Invalidate(key interface{}) {
	a.recordsMu.Lock()
	defer a.recordsMu.Unlock()
	a.delete(key)
}

func (a *NearCacheRecordStore) invalidateWithoutLock(key interface{}) {
	a.delete(key)
}

func (a *NearCacheRecordStore) Clear() {
	a.recordsMu.Lock()
	defer a.recordsMu.Unlock()
	a.records = make(map[interface{}]nearcache.Record)
}

func (a *NearCacheRecordStore) Destroy() {
	a.Clear()
}

func (a *NearCacheRecordStore) Size() int {
	a.recordsMu.RLock()
	defer a.recordsMu.RUnlock()
	return len(a.records)
}

func (a *NearCacheRecordStore) Record(key interface{}) (nearcache.Record, bool) {
	a.recordsMu.RLock()
	defer a.recordsMu.RUnlock()
	record, found := a.records[a.nearcacheKey(key)]
	return record, found
}

func (a *NearCacheRecordStore) DoExpiration() {
	a.recordsMu.Lock()
	defer a.recordsMu.Unlock()
	for key, record := range a.records {
		if a.isRecordExpired(record) {
			a.invalidateWithoutLock(key)
		}
	}
}

func (a *NearCacheRecordStore) DoEviction() {
	if !a.evictionDisabled && a.shouldEvict() {
		recordsToBeEvicted := a.findRecordsToBeEvicted()
		a.removeRecords(recordsToBeEvicted)
	}
}

func (a *NearCacheRecordStore) shouldEvict() bool {
	a.recordsMu.RLock()
	defer a.recordsMu.RUnlock()
	return len(a.records) >= int(a.maxSize)
}

func (a *NearCacheRecordStore) removeRecords(records []nearcache.Record) {
	a.recordsMu.Lock()
	defer a.recordsMu.Unlock()
	for _, record := range records {
		a.delete(record.Key())
	}
}

func (a *NearCacheRecordStore) findRecordsToBeEvicted() []nearcache.Record {
	records := a.createRecordsSlice()
	recordsHolder := recordsHolder{records: records, comparator: a.recordComparator}
	sort.Sort(recordsHolder)
	evictionSize := a.calculateEvictionSize()
	return recordsHolder.records[:evictionSize]
}

func (a *NearCacheRecordStore) calculateEvictionSize() int {
	a.recordsMu.RLock()
	defer a.recordsMu.RUnlock()
	return evictionPercentage * len(a.records) / 100
}

func (n *NearCacheRecordStore) createRecordFromValue(key interface{}, value interface{}) nearcache.Record {
	if n.config.InMemoryFormat() == config.InMemoryFormatObject {
		value = n.toValue(value)
	} else {
		value = n.toData(value)
	}
	creationTime := time.Now()
	if n.timeToLiveDuration > 0 {
		return record.New(key, value, creationTime, creationTime.Add(n.timeToLiveDuration))
	}
	return record.New(key, value, creationTime, nearcache.TimeNotSet)
}

func (a *NearCacheRecordStore) createRecordsSlice() []nearcache.Record {
	records := make([]nearcache.Record, a.Size())
	index := 0
	for _, record := range a.records {
		records[index] = record
		index++
	}
	return records
}

func (a *NearCacheRecordStore) isRecordExpired(record nearcache.Record) bool {
	now := time.Now()
	if record.IsExpiredAt(now) {
		return true
	}
	return record.IsIdleAt(a.maxIdleDuration, now)
}

func (a *NearCacheRecordStore) onRecordAccess(record nearcache.Record) {
	record.SetAccessTime(time.Now())
	record.IncrementAccessHit()
}

func (a *NearCacheRecordStore) onRecordCreate(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	record.SetCreationTime(time.Now())
	a.initInvalidationMetaData(key, keyData, record)
}

func (a *NearCacheRecordStore) initInvalidationMetaData(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	if a.staleReadDetector == nearcache.AlwaysFresh {
		return
	}

	partitionID := a.staleReadDetector.PartitionID(keyData)
	metaDataContainer := a.staleReadDetector.MetaDataContainer(partitionID)
	record.SetPartitionID(partitionID)
	record.SetInvalidationSequence(metaDataContainer.Sequence())
	record.SetUUID(metaDataContainer.UUID())
}

func (a *NearCacheRecordStore) putRecord(key interface{}, record nearcache.Record) nearcache.Record {
	a.recordsMu.Lock()
	defer a.recordsMu.Unlock()
	oldRecord := a.records[a.nearcacheKey(key)]
	a.records[a.nearcacheKey(key)] = record
	return oldRecord
}

func (a *NearCacheRecordStore) StaleReadDetector() nearcache.StaleReadDetector {
	return a.staleReadDetector
}

func (a *NearCacheRecordStore) toData(value interface{}) serialization.Data {
	data, _ := a.serializationService.ToData(value)
	return data
}

func (a *NearCacheRecordStore) toValue(obj interface{}) interface{} {
	if data, ok := obj.(serialization.Data); ok {
		value, _ := a.serializationService.ToObject(data)
		return value
	}
	return obj
}

func (a *NearCacheRecordStore) recordToValue(record nearcache.Record) interface{} {
	return a.toValue(record.Value())
}

func (a *NearCacheRecordStore) nearcacheKey(key interface{}) interface{} {
	if a.config.IsSerializeKeys() {
		keyData := a.toData(key)
		return string(keyData.Buffer())
	}
	return key
}
