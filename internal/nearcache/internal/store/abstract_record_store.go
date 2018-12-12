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

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type AbstractNearCacheRecordStore struct {
	RecordToValue        func(record nearcache.Record) interface{}
	ValueToRecord        func(value interface{}) nearcache.Record
	records              map[interface{}]nearcache.Record
	config               *config.NearCacheConfig
	serializationService spi.SerializationService
	maxIdleDuration      time.Duration
	timeToLiveDuration   time.Duration
}

func newAbstractNearCacheRecordStore(nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService) *AbstractNearCacheRecordStore {
	return &AbstractNearCacheRecordStore{
		records:              make(map[interface{}]nearcache.Record),
		config:               nearCacheCfg,
		serializationService: service,
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
		value := a.RecordToValue(record)
		return value
	}
	return nil
}

func (a *AbstractNearCacheRecordStore) Put(key interface{}, keyData serialization.Data, value interface{},
	valueData serialization.Data) {

	if !a.isAvailable() {
		return
	}
	record := a.ValueToRecord(value)
	a.onRecordCreate(key, keyData, record)
	a.putRecord(key, record)
}

func (a *AbstractNearCacheRecordStore) TryReserveForUpdate(key interface{}, keyData serialization.Data) (reservationID int64, reserved bool) {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) TryPublishReserved(key interface{}, value interface{}, reservationID int64, deserialize bool) (interface{}, bool) {
	panic("implement me")
}

func (a *AbstractNearCacheRecordStore) Invalidate(key interface{}) {
	panic("implement me")
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
	panic("implement me")
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
