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
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/record"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type NearCacheDataRecordStore struct {
	*AbstractNearCacheRecordStore
}

func NewNearCacheDataRecordStore(nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService) *NearCacheDataRecordStore {
	dataRecordStore := &NearCacheDataRecordStore{}
	abstractStore := newAbstractNearCacheRecordStore(nearCacheCfg, service)
	abstractStore.createRecordFromValue = dataRecordStore.createRecordFromValue
	abstractStore.updateRecordValue = dataRecordStore.updateRecordValue
	dataRecordStore.AbstractNearCacheRecordStore = abstractStore
	return dataRecordStore
}

func (n *NearCacheDataRecordStore) createRecordFromValue(key interface{}, value interface{}) nearcache.Record {
	dataValue := n.toData(value)
	creationTime := time.Now()
	if n.timeToLiveDuration > 0 {
		return record.NewNearCacheDataRecord(key, dataValue, creationTime, creationTime.Add(n.timeToLiveDuration))
	}
	return record.NewAbstractNearCacheRecord(key, dataValue, creationTime, nearcache.TimeNotSet)
}

func (n *NearCacheDataRecordStore) updateRecordValue(record nearcache.Record, value interface{}) {
	record.SetValue(n.toData(value))
}
