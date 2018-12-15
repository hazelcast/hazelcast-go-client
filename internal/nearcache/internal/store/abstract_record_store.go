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

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/invalidation"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

const evictionPercentage = 20

type recordsSlice []nearcache.Record

func (r recordsSlice) Len() int {
	return len(r)
}

func (r recordsSlice) Less(i, j int) bool {
	return r[i].LessThan(r[j])
}

func (r recordsSlice) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type AbstractNearCacheRecordStore struct {
	createRecordFromValue func(value interface{}) nearcache.Record
	records               map[interface{}]nearcache.Record
	config                *config.NearCacheConfig
	serializationService  spi.SerializationService
	maxIdleDuration       time.Duration
	timeToLiveDuration    time.Duration
	staleReadDetector     invalidation.StaleReadDetector
	evictionDisabled      bool
	evictionPolicy        config.EvictionPolicy
	maxSize               int
}

func newAbstractNearCacheRecordStore(nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService) *AbstractNearCacheRecordStore {
	return &AbstractNearCacheRecordStore{
		records:              make(map[interface{}]nearcache.Record),
		config:               nearCacheCfg,
		serializationService: service,
		staleReadDetector:    invalidation.AlwaysFresh,
	}
}

func (a *AbstractNearCacheRecordStore) Get(key interface{}) interface{} {
	if !a.isAvailable() {
		return nil
	}
	record := a.Record(key)
	if record != nil {
		if record.RecordState() != nearcache.ReadPermitted {
			return nil
		}
		if a.isRecordExpired(record) {
			return nil
		}
		a.onRecordAccess(record)
		value := a.recordToValue(record)
		return value
	}
	return nil
}

func (a *AbstractNearCacheRecordStore) Put(key interface{}, value interface{}) {

	if !a.isAvailable() {
		return
	}

	if a.evictionPolicy == config.EvictionPolicyNone && len(a.records) >= a.maxSize && !a.containsKey(key) {
		return
	}

	keyData := a.toData(key)
	record := a.createRecordFromValue(value)
	a.onRecordCreate(key, keyData, record)
	a.putRecord(key, record)
}

func (a *AbstractNearCacheRecordStore) containsKey(key interface{}) bool {
	_, found := a.records[key]
	return found
}

func (a *AbstractNearCacheRecordStore) TryReserveForUpdate(key interface{}, keyData serialization.Data) (reservationID int64, reserved bool) {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) TryPublishReserved(key interface{}, value interface{}, reservationID int64, deserialize bool) (interface{}, bool) {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) Invalidate(key interface{}) {
	delete(a.records, key)
}

func (a *AbstractNearCacheRecordStore) Clear() {
	if !a.isAvailable() {
		return
	}
	a.records = make(map[interface{}]nearcache.Record)
}

func (a *AbstractNearCacheRecordStore) Destroy() {
	a.Clear()
}

func (a *AbstractNearCacheRecordStore) Size() int {
	if !a.isAvailable() {
		return -1
	}
	return len(a.records)
}

func (a *AbstractNearCacheRecordStore) Record(key interface{}) nearcache.Record {
	return a.records[key]
}

func (a *AbstractNearCacheRecordStore) DoExpiration() {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) DoEviction(withoutMaxSizeCheck bool) {
	if !a.evictionDisabled {
		recordsToBeEvicted := a.findRecordsToBeEvicted()
		a.removeRecords(recordsToBeEvicted)
	}
}

func (a *AbstractNearCacheRecordStore) removeRecords(records recordsSlice) {
	for _, record := range records {
		delete(a.records, record.Key())
	}
}

func (a *AbstractNearCacheRecordStore) findRecordsToBeEvicted() recordsSlice {
	records := a.createRecordsSlice()
	sort.Sort(records)
	evictionSize := a.calculateEvictionSize()
	return records[:evictionSize]
}

func (a *AbstractNearCacheRecordStore) calculateEvictionSize() int {
	return evictionPercentage * len(a.records) / 100
}

func (a *AbstractNearCacheRecordStore) createRecordsSlice() recordsSlice {
	records := make([]nearcache.Record, len(a.records))
	index := 0
	for _, record := range a.records {
		records[index] = record
		index++
	}
	return records
}

func (a *AbstractNearCacheRecordStore) Initialize() {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) isRecordExpired(record nearcache.Record) bool {
	now := time.Now()
	if record.IsExpiredAt(now) {
		return true
	}
	return record.IsIdleAt(a.maxIdleDuration, now)
}

func (a *AbstractNearCacheRecordStore) onRecordAccess(record nearcache.Record) {
	record.SetAccessTime(time.Now())
	record.IncrementAccessHit()
}

func (a *AbstractNearCacheRecordStore) onRecordCreate(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	record.SetCreationTime(time.Now())
	a.initInvalidationMetaData(key, keyData, record)
}

func (a *AbstractNearCacheRecordStore) isAvailable() bool {
	return a.records != nil
}

func (a *AbstractNearCacheRecordStore) initInvalidationMetaData(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	if a.staleReadDetector == invalidation.AlwaysFresh {
		return
	}

	partitionID := a.staleReadDetector.PartitionID(keyData)
	metaDataContainer := a.staleReadDetector.MetaDataContainer(partitionID)
	record.SetPartitionID(partitionID)
	record.SetInvalidationSequence(metaDataContainer.Sequence())
	record.SetUUID(metaDataContainer.UUID())
}

func (a *AbstractNearCacheRecordStore) putRecord(key interface{}, record nearcache.Record) nearcache.Record {
	oldRecord := a.records[key]
	a.records[key] = record
	return oldRecord
}

func (a *AbstractNearCacheRecordStore) toData(value interface{}) serialization.Data {
	data, _ := a.serializationService.ToData(value)
	return data
}

func (a *AbstractNearCacheRecordStore) toValue(obj interface{}) interface{} {
	if data, ok := obj.(serialization.Data); ok {
		value, _ := a.serializationService.ToObject(data)
		return value
	}
	return obj
}

func (a *AbstractNearCacheRecordStore) recordToValue(record nearcache.Record) interface{} {
	return a.toValue(record.Value())
}
