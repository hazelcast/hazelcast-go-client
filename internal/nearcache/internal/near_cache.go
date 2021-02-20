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
	"sync/atomic"
	"time"

	"sync"

	"sort"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record/comparator"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

const evictionPercentage = 20

var (
	ExpirationTaskInitialDelaySeconds = property.NewHazelcastPropertyInt64WithTimeUnit(
		"hazelcast.internal.nearcache.expiration.task.initial.delay.seconds", 5, time.Second,
	)
	ExpirationTaskPeriodSeconds = property.NewHazelcastPropertyInt64WithTimeUnit(
		"hazelcast.internal.nearcache.expiration.task.period.seconds", 5, time.Second,
	)
)

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

type NearCache struct {
	name                 string
	config               *config.NearCacheConfig
	serializationService spi.SerializationService
	properties           *property.HazelcastProperties
	serializeKeys        bool
	closed               chan struct{}
	recordsMu            sync.RWMutex
	records              map[interface{}]nearcache.Record
	maxIdleDuration      time.Duration
	timeToLiveDuration   time.Duration
	staleReadDetector    nearcache.StaleReadDetector
	evictionDisabled     bool
	evictionPolicy       config.EvictionPolicy
	maxSize              int32
	recordComparator     nearcache.RecordComparator
	reservationID        int64
}

func NewNearCache(name string, nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService, properties *property.HazelcastProperties) *NearCache {
	d := &NearCache{
		name:                 name,
		config:               nearCacheCfg,
		properties:           properties,
		serializationService: service,
		serializeKeys:        nearCacheCfg.IsSerializeKeys(),
		records:              make(map[interface{}]nearcache.Record),
		staleReadDetector:    nearcache.AlwaysFresh,
		maxSize:              nearCacheCfg.MaxEntryCount(),
		evictionPolicy:       nearCacheCfg.EvictionPolicy(),
		evictionDisabled:     nearCacheCfg.EvictionPolicy() == config.EvictionPolicyNone,
		maxIdleDuration:      nearCacheCfg.MaxIdleDuration(),
		timeToLiveDuration:   nearCacheCfg.TimeToLive(),
		closed:               make(chan struct{}, 0),
	}
	d.initRecordComparator()
	return d
}

func (d *NearCache) initRecordComparator() {
	if d.evictionPolicy == config.EvictionPolicyLru {
		d.recordComparator = &comparator.LRUComparator{}
	}
	if d.evictionPolicy == config.EvictionPolicyLfu {
		d.recordComparator = &comparator.LFUComparator{}
	}
}

func (d *NearCache) doEviction() {
	if !d.evictionDisabled && d.shouldEvict() {
		recordsToBeEvicted := d.findRecordsToBeEvicted()
		d.removeRecords(recordsToBeEvicted)
	}
}

func (d *NearCache) shouldEvict() bool {
	d.recordsMu.RLock()
	defer d.recordsMu.RUnlock()
	return len(d.records) >= int(d.maxSize)
}

func (d *NearCache) findRecordsToBeEvicted() []nearcache.Record {
	records := d.createRecordsSlice()
	recordsHolder := recordsHolder{records: records, comparator: d.recordComparator}
	sort.Sort(recordsHolder)
	evictionSize := d.calculateEvictionSize()
	return recordsHolder.records[:evictionSize]
}

func (d *NearCache) removeRecords(records []nearcache.Record) {
	d.recordsMu.Lock()
	defer d.recordsMu.Unlock()
	for _, record := range records {
		d.delete(record.Key())
	}
}

func (d *NearCache) delete(key interface{}) {
	delete(d.records, d.nearcacheKey(key))
}

func (d *NearCache) nearcacheKey(key interface{}) interface{} {
	if d.config.IsSerializeKeys() {
		keyData := d.toData(key)
		return string(keyData.Buffer())
	}
	return key
}

func (d *NearCache) toData(value interface{}) serialization.Data {
	data, _ := d.serializationService.ToData(value)
	return data
}

func (d *NearCache) toValue(obj interface{}) interface{} {
	if data, ok := obj.(serialization.Data); ok {
		value, _ := d.serializationService.ToObject(data)
		return value
	}
	return obj
}

func (d *NearCache) createRecordsSlice() []nearcache.Record {
	records := make([]nearcache.Record, d.Size())
	index := 0
	for _, record := range d.records {
		records[index] = record
		index++
	}
	return records
}

func (d *NearCache) calculateEvictionSize() int {
	d.recordsMu.RLock()
	defer d.recordsMu.RUnlock()
	return evictionPercentage * len(d.records) / 100
}

func (d *NearCache) TryReserveForUpdate(key interface{}, keyData serialization.Data) (int64, bool) {
	d.doEviction()
	return d.tryReserveForUpdate(key, keyData)
}

func (d *NearCache) tryReserveForUpdate(key interface{},
	keyData serialization.Data) (reservationID int64, reserved bool) {
	if d.evictionPolicy == config.EvictionPolicyNone && d.Size() >= int(d.maxSize) && !d.containsKey(key) {
		return 0, false
	}
	reservedRecord := d.getOrCreateToReserve(key)
	reservationID = d.nextReservationID()
	if reservedRecord.CasRecordState(nearcache.Reserved, reservationID) {
		return reservationID, true
	}
	return 0, false
}

func (d *NearCache) nextReservationID() int64 {
	return atomic.AddInt64(&d.reservationID, 1)
}

func (d *NearCache) getOrCreateToReserve(key interface{}) nearcache.Record {
	d.recordsMu.RLock()
	if record, found := d.records[d.nearcacheKey(key)]; found {
		defer d.recordsMu.RUnlock()
		return record
	}
	d.recordsMu.RUnlock()
	keyData, _ := d.serializationService.ToData(key)
	record := d.createRecordFromValue(key, nil)
	d.onRecordCreate(key, keyData, record)
	record.CasRecordState(nearcache.ReadPermitted, nearcache.Reserved)
	d.putRecord(key, record)
	return record
}

func (d *NearCache) onRecordAccess(record nearcache.Record) {
	record.SetAccessTime(time.Now())
	record.IncrementAccessHit()
}

func (d *NearCache) onRecordCreate(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	record.SetCreationTime(time.Now())
	d.initInvalidationMetaData(key, keyData, record)
}

func (d *NearCache) initInvalidationMetaData(key interface{},
	keyData serialization.Data, record nearcache.Record) {
	if d.staleReadDetector == nearcache.AlwaysFresh {
		return
	}

	partitionID := d.staleReadDetector.PartitionID(keyData)
	metaDataContainer := d.staleReadDetector.MetaDataContainer(partitionID)
	record.SetPartitionID(partitionID)
	record.SetInvalidationSequence(metaDataContainer.Sequence())
	record.SetUUID(metaDataContainer.UUID())
}

func (d *NearCache) putRecord(key interface{}, record nearcache.Record) nearcache.Record {
	d.recordsMu.Lock()
	defer d.recordsMu.Unlock()
	oldRecord := d.records[d.nearcacheKey(key)]
	d.records[d.nearcacheKey(key)] = record
	return oldRecord
}

func (d *NearCache) createRecordFromValue(key interface{}, value interface{}) nearcache.Record {
	if d.config.InMemoryFormat() == config.InMemoryFormatObject {
		value = d.toValue(value)
	} else {
		value = d.toData(value)
	}
	creationTime := time.Now()
	if d.timeToLiveDuration > 0 {
		return record.New(key, value, creationTime, creationTime.Add(d.timeToLiveDuration))
	}
	return record.New(key, value, creationTime, nearcache.TimeNotSet)
}

func (d *NearCache) containsKey(key interface{}) bool {
	d.recordsMu.RLock()
	defer d.recordsMu.RUnlock()
	_, found := d.records[d.nearcacheKey(key)]
	return found
}

func (d *NearCache) TryPublishReserved(key interface{}, value interface{}, reservationID int64,
	deserialize bool) (interface{}, bool) {
	return d.tryPublishReserved(key, value, reservationID, deserialize)
}

func (d *NearCache) tryPublishReserved(key interface{},
	value interface{}, reservationID int64, deserialize bool) (interface{}, bool) {
	d.recordsMu.RLock()
	reservedRecord, found := d.records[d.nearcacheKey(key)]
	d.recordsMu.RUnlock()
	if !found {
		return nil, false
	}
	if !reservedRecord.CasRecordState(reservationID, nearcache.UpdateStarted) {
		return nil, false
	}
	d.updateRecordValue(reservedRecord, value)
	reservedRecord.CasRecordState(nearcache.UpdateStarted, nearcache.ReadPermitted)
	cachedValue := reservedRecord.Value()
	return d.toValue(cachedValue), true
}

func (d *NearCache) updateRecordValue(record nearcache.Record, value interface{}) {
	if d.config.InMemoryFormat() == config.InMemoryFormatObject {
		record.SetValue(d.toValue(value))
	} else {
		record.SetValue(d.toData(value))
	}
}

func (d *NearCache) Get(key interface{}) interface{} {
	if record, found := d.Record(key); found {
		if record.RecordState() != nearcache.ReadPermitted {
			return nil
		}
		if d.staleReadDetector.IsStaleRead(key, record) {
			d.Invalidate(key)
			return nil
		}
		if d.isRecordExpired(record) {
			d.Invalidate(key)
			return nil
		}
		d.onRecordAccess(record)
		value := d.recordToValue(record)
		return value
	}
	return nil
}

func (d *NearCache) isRecordExpired(record nearcache.Record) bool {
	now := time.Now()
	if record.IsExpiredAt(now) {
		return true
	}
	return record.IsIdleAt(d.maxIdleDuration, now)
}

func (d *NearCache) recordToValue(record nearcache.Record) interface{} {
	return d.toValue(record.Value())
}

func (d *NearCache) Record(key interface{}) (nearcache.Record, bool) {
	d.recordsMu.RLock()
	defer d.recordsMu.RUnlock()
	record, found := d.records[d.nearcacheKey(key)]
	return record, found
}

func (d *NearCache) Size() int {
	d.recordsMu.RLock()
	defer d.recordsMu.RUnlock()
	return len(d.records)
}

func (d *NearCache) Clear() {
	d.recordsMu.Lock()
	defer d.recordsMu.Unlock()
	d.records = make(map[interface{}]nearcache.Record)
}

func (d *NearCache) Invalidate(key interface{}) {
	d.recordsMu.Lock()
	defer d.recordsMu.Unlock()
	d.delete(key)
}

func (d *NearCache) invalidateWithoutLock(key interface{}) {
	d.delete(key)
}

func (d *NearCache) Put(key interface{}, value interface{}) {
	d.doEviction()
	d.put(key, value)
}

func (d *NearCache) put(key interface{}, value interface{}) {
	if d.evictionPolicy == config.EvictionPolicyNone && d.Size() >= int(d.maxSize) && !d.containsKey(key) {
		return
	}

	keyData := d.toData(key)
	record := d.createRecordFromValue(key, value)
	d.onRecordCreate(key, keyData, record)
	d.putRecord(key, record)
}

func (d *NearCache) SetStaleReadDetector(detector nearcache.StaleReadDetector) {
	d.staleReadDetector = detector
}

func (d *NearCache) Initialize() {
	go d.doExpirationPeriodically()
}

func (d *NearCache) doExpirationPeriodically() {
	if d.config.MaxIdleDuration() > 0 || d.config.TimeToLive() > 0 {
		initialWait := d.properties.GetPositiveDurationOrDef(ExpirationTaskInitialDelaySeconds)
		time.Sleep(initialWait)
		period := d.properties.GetPositiveDurationOrDef(ExpirationTaskPeriodSeconds)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		d.doExpiration()
		for {
			select {
			case <-ticker.C:
				d.doExpiration()
			case <-d.closed:
				return
			}
		}
	}
}

func (d *NearCache) doExpiration() {
	d.recordsMu.Lock()
	defer d.recordsMu.Unlock()
	for key, record := range d.records {
		if d.isRecordExpired(record) {
			d.invalidateWithoutLock(key)
		}
	}
}

func (d *NearCache) StaleReadDetector() nearcache.StaleReadDetector {
	return d.staleReadDetector
}

func (d *NearCache) Destroy() {
	close(d.closed)
	d.Clear()
}
