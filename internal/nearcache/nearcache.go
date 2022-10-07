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
	"os"
	"sync/atomic"
	"time"

	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type UpdateSemantic int8

const (
	UpdateSemanticReadUpdate UpdateSemantic = iota
	UpdateSemanticWriteUpdate
)

const (
	// see: com.hazelcast.internal.nearcache.NearCache#DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS
	defaultExpirationTaskInitialDelay = 5 * time.Second
	// see: com.hazelcast.internal.nearcache.NearCache#DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS
	defaultExpirationTaskPeriod   = 5 * time.Second
	EnvExpirationTaskInitialDelay = "TESTONLY_NC_EXPIRATION_INITIAL_DELAY"
	EnvExpirationTaskPeriod       = "TESTONLY_NC_EXPIRATION_TASK_PERIOD"
)

type NearCache struct {
	store  *RecordStore
	cfg    *nearcache.Config
	lg     ilogger.LogAdaptor
	doneCh chan struct{}
	state  int32
}

func NewNearCache(cfg *nearcache.Config, ss *serialization.Service, lg ilogger.LogAdaptor) *NearCache {
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
	nc := &NearCache{
		cfg:    cfg,
		store:  NewRecordStore(cfg, ss, rc, se),
		lg:     lg,
		doneCh: make(chan struct{}),
	}
	if cfg.TimeToLiveSeconds > 0 || cfg.MaxIdleSeconds > 0 {
		delay := nc.parseDurationOrDefault(EnvExpirationTaskInitialDelay, defaultExpirationTaskInitialDelay)
		period := nc.parseDurationOrDefault(EnvExpirationTaskPeriod, defaultExpirationTaskPeriod)
		go nc.startExpirationTask(delay, period)
	}
	return nc
}

func (nc *NearCache) Config() *nearcache.Config {
	return nc.cfg
}

func (nc *NearCache) Clear() {
	nc.store.Clear()
}

func (nc *NearCache) Destroy() {
	if atomic.CompareAndSwapInt32(&nc.state, 0, 1) {
		close(nc.doneCh)
		nc.store.Destroy()
	}
}

func (nc *NearCache) Get(key interface{}) (interface{}, bool, error) {
	nc.checkKeyFormat(key)
	return nc.store.Get(key)
}

func (nc *NearCache) GetRecord(key interface{}) (*Record, bool) {
	// this function is exported only for tests.
	// do not use outside of tests.
	nc.checkKeyFormat(key)
	return nc.store.GetRecord(key)
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
	// port of: com.hazelcast.internal.nearcache.impl.DefaultNearCache#tryReserveForUpdate
	nc.store.doEviction()
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

func (nc *NearCache) startExpirationTask(delay, timeout time.Duration) {
	time.Sleep(delay)
	timer := time.NewTicker(timeout)
	defer timer.Stop()
	for {
		select {
		case <-nc.doneCh:
			return
		case <-timer.C:
			nc.lg.Debug(func() string {
				return "running near cache expiration task"
			})
			nc.store.DoExpiration()
		}
	}
}

func (nc *NearCache) parseDurationOrDefault(envName string, d time.Duration) time.Duration {
	str := os.Getenv(envName)
	if str == "" {
		return d
	}
	dur, err := time.ParseDuration(str)
	if err != nil {
		nc.lg.Warnf("nearcache.NearCache.parseDurationOrDefault: ignoring %s", envName)
		return d
	}
	return dur
}
