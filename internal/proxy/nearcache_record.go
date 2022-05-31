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

package proxy

import (
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	NumberOfLongFieldTypes    = 2
	NumberOfIntegerFieldTypes = 5
	NumberOfBooleanFieldTypes = 1
)

type NearCacheRecord struct {
	CreationTime         int32
	Value                interface{}
	UUID                 types.UUID
	CachedAsNil          bool
	PartitionID          int32
	LastAccessTime       int32
	ExpirationTime       int32
	InvalidationSequence int64
	reservationID        int64
	hits                 int32
}

func NewNearCacheRecord(value interface{}, creationTime, expirationTime int32) NearCacheRecord {
	return NearCacheRecord{
		CreationTime:   creationTime,
		Value:          value,
		ExpirationTime: expirationTime,
	}
}

func (r *NearCacheRecord) Hits() int32 {
	return atomic.LoadInt32(&r.hits)
}

func (r *NearCacheRecord) SetHits(value int32) {
	atomic.StoreInt32(&r.hits, value)
}

func (r *NearCacheRecord) IncrementHits() {
	atomic.AddInt32(&r.hits, 1)
}

func (r *NearCacheRecord) ReservationID() int64 {
	return atomic.LoadInt64(&r.reservationID)
}

func (r *NearCacheRecord) SetReservationID(rid int64) {
	atomic.StoreInt64(&r.reservationID, rid)
}
