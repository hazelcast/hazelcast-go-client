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

type NearCacheObjectRecordStore struct {
	*AbstractNearCacheRecordStore
}

func NewNearCacheObjectRecordStore(nearCacheCfg *config.NearCacheConfig,
	service spi.SerializationService) *NearCacheObjectRecordStore {
	objectRecordStore := &NearCacheObjectRecordStore{}
	abstractStore := newAbstractNearCacheRecordStore(nearCacheCfg, service)
	abstractStore.createRecordFromValue = objectRecordStore.createRecordFromValue
	objectRecordStore.AbstractNearCacheRecordStore = abstractStore
	return objectRecordStore
}

func (n *NearCacheObjectRecordStore) createRecordFromValue(key, value interface{}) nearcache.Record {
	value = n.toValue(value)
	creationTime := time.Now()
	if n.timeToLiveDuration > 0 {
		return record.NewNearCacheObjectRecord(key, value, creationTime, creationTime.Add(n.timeToLiveDuration))
	}
	return record.NewNearCacheObjectRecord(key, value, creationTime, nearcache.TimeNotSet)
}
