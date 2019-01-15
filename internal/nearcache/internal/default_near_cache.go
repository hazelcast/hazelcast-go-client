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

package internal

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache/internal/store"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

var (
	ExpirationTaskInitialDelaySeconds = property.NewHazelcastPropertyInt64WithTimeUnit(
		"hazelcast.internal.nearcache.expiration.task.initial.delay.seconds", 5, time.Second,
	)
	ExpirationTaskPeriodSeconds = property.NewHazelcastPropertyInt64WithTimeUnit(
		"hazelcast.internal.nearcache.expiration.task.period.seconds", 5, time.Second,
	)
)

type DefaultNearCache struct {
	name                 string
	config               *config.NearCacheConfig
	serializationService spi.SerializationService
	properties           *property.HazelcastProperties
	serializeKeys        bool
	nearCacheRecordStore nearcache.RecordStore
	closed               chan struct{}
}

func NewDefaultNearCache(name string, config *config.NearCacheConfig,
	service spi.SerializationService, properties *property.HazelcastProperties) *DefaultNearCache {
	return &DefaultNearCache{
		name:                 name,
		config:               config,
		properties:           properties,
		serializationService: service,
		serializeKeys:        config.IsSerializeKeys(),
		closed:               make(chan struct{}, 0),
	}
}

func (d *DefaultNearCache) TryReserveForUpdate(key interface{}, keyData serialization.Data) (int64, bool) {
	d.nearCacheRecordStore.DoEviction()
	return d.nearCacheRecordStore.TryReserveForUpdate(key, keyData)
}

func (d *DefaultNearCache) TryPublishReserved(key interface{}, value interface{}, reservationID int64,
	deserialize bool) (interface{}, bool) {
	return d.nearCacheRecordStore.TryPublishReserved(key, value, reservationID, deserialize)
}

func (d *DefaultNearCache) Get(key interface{}) interface{} {
	return d.nearCacheRecordStore.Get(key)
}

func (d *DefaultNearCache) Size() int {
	return d.nearCacheRecordStore.Size()
}

func (d *DefaultNearCache) Clear() {
	d.nearCacheRecordStore.Clear()
}

func (d *DefaultNearCache) Invalidate(key interface{}) {
	d.nearCacheRecordStore.Invalidate(key)
}

func (d *DefaultNearCache) Put(key interface{}, value interface{}) {
	d.nearCacheRecordStore.DoEviction()
	d.nearCacheRecordStore.Put(key, value)
}

func (d *DefaultNearCache) SetStaleReadDetector(detector nearcache.StaleReadDetector) {
	d.nearCacheRecordStore.SetStaleReadDetector(detector)
}

func (d *DefaultNearCache) Initialize() {
	if d.nearCacheRecordStore == nil {
		d.nearCacheRecordStore = d.createNearCacheRecordStore(d.name, d.config)
	}
	go d.doExpirationPeriodically()
}

func (d *DefaultNearCache) doExpirationPeriodically() {
	if d.config.MaxIdleDuration() > 0 || d.config.TimeToLive() > 0 {
		initialWait := d.properties.GetPositiveDurationOrDef(ExpirationTaskInitialDelaySeconds)
		time.Sleep(initialWait)
		period := d.properties.GetPositiveDurationOrDef(ExpirationTaskPeriodSeconds)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		d.nearCacheRecordStore.DoExpiration()
		for {
			select {
			case <-ticker.C:
				d.nearCacheRecordStore.DoExpiration()
			case <-d.closed:
				return
			}
		}
	}
}

func (d *DefaultNearCache) Store() nearcache.RecordStore {
	return d.nearCacheRecordStore
}

func (d *DefaultNearCache) Destroy() {
	close(d.closed)
	d.nearCacheRecordStore.Destroy()
}

func (d *DefaultNearCache) createNearCacheRecordStore(name string, cfg *config.NearCacheConfig) nearcache.RecordStore {
	return store.New(cfg, d.serializationService)
}
