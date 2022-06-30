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
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Record struct {
	CreationTime         int64
	lastAccessTime       int64
	ExpirationTime       int64
	InvalidationSequence int64
	reservationID        int64
	value                internal.AtomicValue
	UUID                 types.UUID
	hits                 int32
	cachedAsNil          int32
	PartitionID          int32
}

func NewRecord(value interface{}, creationTime, expirationTime int64) *Record {
	av := internal.AtomicValue{}
	av.Store(&value)
	return &Record{
		CreationTime:   creationTime,
		value:          av,
		ExpirationTime: expirationTime,
	}
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

func (r *Record) LastAccessTimeMS() int64 {
	return atomic.LoadInt64(&r.lastAccessTime)
}

func (r *Record) SetLastAccessTimeMS(ms int64) {
	atomic.StoreInt64(&r.lastAccessTime, ms)
}

func (r *Record) IsExpiredAtMS(ms int64) bool {
	return r.ExpirationTime > 0 && r.ExpirationTime <= ms
}

func (r *Record) IsIdleAtMS(maxIdleMS, nowMS int64) bool {
	if maxIdleMS <= 0 {
		return false
	}
	lat := r.LastAccessTimeMS()
	if lat > 0 {
		return lat+maxIdleMS < nowMS
	}
	return r.CreationTime+maxIdleMS < nowMS
}

func (r *Record) CachedAsNil() bool {
	return atomic.LoadInt32(&r.cachedAsNil) == 1
}

func (r *Record) SetCachedAsNil() {
	atomic.StoreInt32(&r.cachedAsNil, 1)
}
