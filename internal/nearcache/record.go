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
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Record struct {
	invalidationSequence int64
	reservationID        int64
	value                internal.AtomicValue
	uuid                 atomic.Value
	creationTime         int32
	lastAccessTime       int32
	expirationTime       int32
	hits                 int32
	cachedAsNil          int32
	partitionID          int32
}

func NewRecord(value interface{}, creationTime, expirationTime int64) *Record {
	av := internal.AtomicValue{}
	av.Store(&value)
	rec := &Record{value: av}
	rec.SetCreationTime(creationTime)
	rec.SetExpirationTIme(expirationTime)
	return rec
}

func (r *Record) Value() interface{} {
	return r.value.Load()
}

func (r *Record) SetValue(value interface{}) {
	r.value.Store(&value)
}

func (r *Record) Hits() int32 {
	return atomic.LoadInt32(&r.hits)
}

func (r *Record) SetHits(value int32) {
	atomic.StoreInt32(&r.hits, value)
}

func (r *Record) IncrementHits() {
	atomic.AddInt32(&r.hits, 1)
}

func (r *Record) ReservationID() int64 {
	return atomic.LoadInt64(&r.reservationID)
}

func (r *Record) SetReservationID(rid int64) {
	atomic.StoreInt64(&r.reservationID, rid)
}

func (r *Record) CreationTime() int64 {
	t := atomic.LoadInt32(&r.creationTime)
	return RecomputeWithBaseTime(t)
}

func (r *Record) SetCreationTime(ms int64) {
	secs := StripBaseTime(ms)
	atomic.StoreInt32(&r.creationTime, secs)
}

func (r *Record) LastAccessTime() int64 {
	t := atomic.LoadInt32(&r.lastAccessTime)
	return RecomputeWithBaseTime(t)
}

func (r *Record) SetLastAccessTime(ms int64) {
	secs := StripBaseTime(ms)
	atomic.StoreInt32(&r.lastAccessTime, secs)
}

func (r *Record) ExpirationTime() int64 {
	t := atomic.LoadInt32(&r.expirationTime)
	return RecomputeWithBaseTime(t)
}

func (r *Record) SetExpirationTIme(ms int64) {
	secs := StripBaseTime(ms)
	atomic.StoreInt32(&r.expirationTime, secs)
}

func (r *Record) IsExpiredAt(ms int64) bool {
	t := r.ExpirationTime()
	return t > 0 && t <= ms
}

func (r *Record) IsIdleAt(maxIdleMS, nowMS int64) bool {
	if maxIdleMS <= 0 {
		return false
	}
	lat := r.LastAccessTime()
	if lat > 0 {
		return lat+maxIdleMS < nowMS
	}
	return r.CreationTime()+maxIdleMS < nowMS
}

func (r *Record) CachedAsNil() bool {
	return atomic.LoadInt32(&r.cachedAsNil) == 1
}

func (r *Record) SetCachedAsNil() {
	atomic.StoreInt32(&r.cachedAsNil, 1)
}

func (r *Record) InvalidationSequence() int64 {
	return atomic.LoadInt64(&r.invalidationSequence)
}

func (r *Record) SetInvalidationSequence(v int64) {
	atomic.StoreInt64(&r.invalidationSequence, v)
}

func (r *Record) PartitionID() int32 {
	return atomic.LoadInt32(&r.partitionID)
}

func (r *Record) SetPartitionID(v int32) {
	atomic.StoreInt32(&r.partitionID, v)
}

func (r *Record) UUID() types.UUID {
	v := r.uuid.Load()
	if v == nil {
		return types.UUID{}
	}
	return v.(types.UUID)
}

func (r *Record) SetUUID(v types.UUID) {
	r.uuid.Store(v)
}
