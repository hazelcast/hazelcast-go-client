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

package record

import (
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
)

type NearCacheRecord struct {
	partitionID    int32
	sequence       int64
	uuid           atomic.Value
	value          atomic.Value
	key            interface{}
	expirationTime atomic.Value
	creationTime   atomic.Value
	lastAccessTime atomic.Value
	recordState    int64
	accessHit      int32
}

func New(key interface{}, value interface{}, creationTime time.Time,
	expirationTime time.Time) *NearCacheRecord {
	a := &NearCacheRecord{}
	a.SetValue(value)
	a.SetKey(key)
	a.creationTime.Store(creationTime)
	a.SetExpirationTime(expirationTime)
	a.lastAccessTime.Store(nearcache.TimeNotSet)
	a.uuid.Store("")
	atomic.StoreInt64(&a.recordState, nearcache.ReadPermitted)
	return a
}

func (a *NearCacheRecord) CreationTime() time.Time {
	return a.creationTime.Load().(time.Time)
}

func (a *NearCacheRecord) LastAccessTime() time.Time {
	return a.lastAccessTime.Load().(time.Time)
}

func (a *NearCacheRecord) AccessHit() int32 {
	return atomic.LoadInt32(&a.accessHit)
}

func (a *NearCacheRecord) ExpirationTime() time.Time {
	return a.expirationTime.Load().(time.Time)
}

func (a *NearCacheRecord) SetExpirationTime(time time.Time) {
	a.expirationTime.Store(time)
}

func (a *NearCacheRecord) IsExpiredAt(atTime time.Time) bool {
	expirationTime := a.ExpirationTime()
	return !expirationTime.Equal(nearcache.TimeNotSet) && expirationTime.Before(atTime)
}

func (a *NearCacheRecord) SetKey(key interface{}) {
	a.key = key
}

func (a *NearCacheRecord) Key() interface{} {
	return a.key
}

func (a *NearCacheRecord) Value() interface{} {
	return a.value.Load()
}

func (a *NearCacheRecord) SetValue(value interface{}) {
	if value != nil {
		a.value.Store(value)
	}
}

func (a *NearCacheRecord) SetCreationTime(time time.Time) {
	a.creationTime.Store(time)
}

func (a *NearCacheRecord) SetAccessTime(time time.Time) {
	a.lastAccessTime.Store(time)
}

func (a *NearCacheRecord) IsIdleAt(maxIdleTime time.Duration, now time.Time) bool {
	if maxIdleTime > 0 {
		accessTime := a.lastAccessTime.Load().(time.Time)
		creationTime := a.creationTime.Load().(time.Time)
		if !accessTime.Equal(nearcache.TimeNotSet) {
			return accessTime.Add(maxIdleTime).Before(now)
		}
		return creationTime.Add(maxIdleTime).Before(now)

	}
	return false
}

func (a *NearCacheRecord) IncrementAccessHit() {
	atomic.AddInt32(&a.accessHit, 1)
}

func (a *NearCacheRecord) RecordState() int64 {
	return atomic.LoadInt64(&a.recordState)
}

func (a *NearCacheRecord) CasRecordState(expect int64, update int64) bool {
	return atomic.CompareAndSwapInt64(&a.recordState, expect, update)
}

func (a *NearCacheRecord) PartitionID() int32 {
	return atomic.LoadInt32(&a.partitionID)
}

func (a *NearCacheRecord) SetPartitionID(partitionID int32) {
	atomic.StoreInt32(&a.partitionID, partitionID)
}

func (a *NearCacheRecord) InvalidationSequence() int64 {
	return atomic.LoadInt64(&a.sequence)
}

func (a *NearCacheRecord) SetInvalidationSequence(sequence int64) {
	atomic.StoreInt64(&a.sequence, sequence)
}

func (a *NearCacheRecord) SetUUID(UUID string) {
	a.uuid.Store(UUID)
}

func (a *NearCacheRecord) HasSameUUID(UUID string) bool {
	uuid := a.uuid.Load().(string)
	return uuid != "" && UUID != "" && uuid == UUID
}

func (a *NearCacheRecord) LessThan(comparator nearcache.RecordComparator, record nearcache.Record) bool {
	return comparator.CompareRecords(a, record)
}
