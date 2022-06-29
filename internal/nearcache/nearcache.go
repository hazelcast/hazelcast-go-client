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
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type UpdateSemantic int8

const (
	UpdateSemanticReadUpdate UpdateSemantic = iota
	UpdateSemanticWriteUpdate
)

type NearCache struct {
	store RecordStore
	cfg   *nearcache.Config
}

func NewNearCache(cfg *nearcache.Config, ss *serialization.Service) *NearCache {
	var rc nearCacheRecordValueConverter
	var se nearCacheStorageEstimator
	if cfg.InMemoryFormat == nearcache.InMemoryFormatBinary {
		adapter := nearCacheDataStoreAdapter{ss: ss}
		rc = adapter
		se = adapter
	} else {
		adapter := nearCacheValueStoreAdapter{ss: ss}
		rc = adapter
		se = adapter
	}
	return &NearCache{
		cfg:   cfg,
		store: NewRecordStore(cfg, ss, rc, se),
	}
}

func (nc *NearCache) Get(key interface{}) (interface{}, bool, error) {
	nc.checkKeyFormat(key)
	return nc.store.Get(key)
}

func (nc *NearCache) Invalidate(key interface{}) {
	nc.checkKeyFormat(key)
	nc.store.Invalidate(key)
}

func (nc NearCache) Size() int {
	return nc.store.Size()
}

func (nc NearCache) Stats() nearcache.Stats {
	return nc.store.Stats()
}

// InvalidationRequests returns the invalidation requests.
// It is used only for tests.
func (nc NearCache) InvalidationRequests() int64 {
	return nc.store.InvalidationRequests()
}

func (nc *NearCache) TryReserveForUpdate(key interface{}, keyData serialization.Data, ups UpdateSemantic) (int64, error) {
	// eviction stuff will be implemented in another PR
	// nearCacheRecordStore.doEviction(false);
	return nc.store.TryReserveForUpdate(key, keyData, ups)
}

func (nc *NearCache) TryPublishReserved(key, value interface{}, reservationID int64) (interface{}, error) {
	cached, err := nc.store.TryPublishReserved(key, value, reservationID, true)
	if err != nil {
		return nil, err
	}
	if cached != nil {
		value = cached
	}
	return value, nil
}

func (nc *NearCache) checkKeyFormat(key interface{}) {
	_, ok := key.(serialization.Data)
	if nc.cfg.SerializeKeys {
		if !ok {
			panic("key must be of type serialization.Data!")
		}
	} else if ok {
		panic("key cannot be of type serialization.Data!")
	}
}
