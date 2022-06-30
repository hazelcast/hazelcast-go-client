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

import "github.com/hazelcast/hazelcast-go-client/internal/serialization"

const (
	RecordNotReserved      int64 = -1
	RecordReadPermitted          = -2
	RecordStoreTimeNotSet  int64 = -1
	pointerCostInBytes           = (32 << uintptr(^uintptr(0)>>63)) >> 3
	int32CostInBytes             = 4
	int64CostInBytes             = 8
	atomicValueCostInBytes       = 8
	uuidCostInBytes              = 16 // low uint64 + high uint64
)

type nearCacheRecordValueConverter interface {
	ConvertValue(value interface{}) (interface{}, error)
}

type nearCacheStorageEstimator interface {
	GetRecordStorageMemoryCost(rec *Record) int64
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

func (n nearCacheDataStoreAdapter) GetRecordStorageMemoryCost(rec *Record) int64 {
	if rec == nil {
		return 0
	}
	cost := pointerCostInBytes + // the record is stored as a pointer in the map
		5*int64CostInBytes + // CreationTime, lastAccessTime, ExpirationTime, InvalidationSequence, reservationID
		3*int32CostInBytes + // PartitionID, hits, cachedAsNil
		1*atomicValueCostInBytes + // holder of the value
		1*uuidCostInBytes // UUID
	cost += rec.Value().(serialization.Data).DataSize()
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

func (n nearCacheValueStoreAdapter) GetRecordStorageMemoryCost(rec *Record) int64 {
	// memory cost for "OBJECT" in memory format is totally not supported, so just return zero
	return 0
}
