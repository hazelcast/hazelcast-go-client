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
	"testing"

	"strconv"
	"time"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
	"github.com/stretchr/testify/assert"
)

func TestAbstractNearCacheRecordStore_Put(t *testing.T) {
	nearCacheConfig := &config.NearCacheConfig{}
	service, _ := spi.NewSerializationService(serialization.NewConfig())
	abstractStore := NewNearCacheObjectRecordStore(nearCacheConfig, service)
	abstractStore.maxSize = 10
	expectedValue := "value"
	abstractStore.Put("key", expectedValue)
	actualValue := abstractStore.Get("key")
	assert.Equal(t, expectedValue, actualValue)
}

func TestAbstractNearCacheRecordStore_DoEvictionLFU(t *testing.T) {
	records := createRecordsWithIncreasingHit(20)
	nearCacheCfg := config.NewNearCacheConfig()
	nearCacheCfg.SetEvictionPolicy(config.EvictionPolicyLfu)
	nearCacheCfg.SetMaxEntryCount(10)
	service, _ := spi.NewSerializationService(serialization.NewConfig())
	abstractStore := NewNearCacheObjectRecordStore(nearCacheCfg, service)
	abstractStore.records = records
	abstractStore.DoEviction()
	expectedRemainingSize := 20 * (100 - evictionPercentage) / 100
	assert.Len(t, abstractStore.records, expectedRemainingSize)
	evictedSize := 20 * (evictionPercentage) / 100
	for i := 0; i < evictedSize; i++ {
		assert.NotContains(t, abstractStore.records, "key"+strconv.Itoa(i))
	}
}

func createRecordsWithIncreasingHit(size int) map[interface{}]nearcache.Record {
	records := make(map[interface{}]nearcache.Record, 0)
	for i := 0; i < size; i++ {
		rec := record.NewAbstractNearCacheRecord("key"+strconv.Itoa(i), "value"+strconv.Itoa(i), time.Now(), time.Now())
		for j := 0; j < i; j++ {
			rec.IncrementAccessHit()
		}
		records["key"+strconv.Itoa(i)] = rec
	}
	return records
}

func TestAbstractNearCacheRecordStore_DoEvictionLRU(t *testing.T) {
	records := createRecordsWithIncreasingLastAccess(20)
	nearCacheCfg := config.NewNearCacheConfig()
	nearCacheCfg.SetEvictionPolicy(config.EvictionPolicyLru)
	nearCacheCfg.SetMaxEntryCount(10)
	service, _ := spi.NewSerializationService(serialization.NewConfig())
	abstractStore := NewNearCacheObjectRecordStore(nearCacheCfg, service)
	abstractStore.records = records
	abstractStore.DoEviction()
	expectedRemainingSize := 20 * (100 - evictionPercentage) / 100
	assert.Len(t, abstractStore.records, expectedRemainingSize)
	evictedSize := 20 * (evictionPercentage) / 100
	for i := 0; i < evictedSize; i++ {
		assert.NotContains(t, abstractStore.records, "key"+strconv.Itoa(i))
	}
}

func createRecordsWithIncreasingLastAccess(size int) map[interface{}]nearcache.Record {
	records := make(map[interface{}]nearcache.Record, 0)
	for i := 0; i < size; i++ {
		rec := record.NewAbstractNearCacheRecord("key"+strconv.Itoa(i), "value"+strconv.Itoa(i), time.Now(), time.Now())
		rec.SetAccessTime(time.Now().Add(time.Duration(i) * time.Second))
		records["key"+strconv.Itoa(i)] = rec
	}
	return records
}

func TestAbstractNearCacheRecordStore_DoExpiration(t *testing.T) {
	size := 20
	records := createRecordsWithHalfExpired(size)
	nearCacheCfg := config.NewNearCacheConfig()
	nearCacheCfg.SetEvictionPolicy(config.EvictionPolicyLru)
	nearCacheCfg.SetMaxEntryCount(50)
	service, _ := spi.NewSerializationService(serialization.NewConfig())
	abstractStore := NewNearCacheObjectRecordStore(nearCacheCfg, service)
	abstractStore.records = records
	abstractStore.DoExpiration()
	expectedRemainingSize := size / 2
	assert.Len(t, abstractStore.records, expectedRemainingSize)
	expiredSize := size - expectedRemainingSize
	for i := 0; i < expiredSize; i++ {
		assert.NotContains(t, abstractStore.records, "key"+strconv.Itoa(i))
	}
}

func createRecordsWithHalfExpired(size int) map[interface{}]nearcache.Record {
	records := make(map[interface{}]nearcache.Record, 0)
	for i := 0; i < size/2; i++ {
		rec := record.NewAbstractNearCacheRecord("key"+strconv.Itoa(i), "value"+strconv.Itoa(i), time.Now(), time.Now())
		records["key"+strconv.Itoa(i)] = rec
	}
	for i := size / 2; i < size; i++ {
		rec := record.NewAbstractNearCacheRecord("key"+strconv.Itoa(i), "value"+strconv.Itoa(i), time.Now(),
			time.Now().Add(time.Hour))
		records["key"+strconv.Itoa(i)] = rec
	}

	return records
}
