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

package it

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type NearCacheTestContext struct {
	T                             *testing.T
	NC                            NearCacheAdapter
	Config                        *nearcache.Config
	DSAdapter                     DataStructureAdapter
	ss                            *serialization.Service
	decrementInvalidationRequests int64
}

func NewNearCacheTestContext(t *testing.T, nc NearCacheAdapter, dsa DataStructureAdapter, cfg *nearcache.Config, ss *serialization.Service) *NearCacheTestContext {
	return &NearCacheTestContext{
		T:         t,
		NC:        nc,
		DSAdapter: dsa,
		Config:    cfg,
		ss:        ss,
	}
}

func (tcx NearCacheTestContext) Stats() (nearcache.Stats, int64) {
	stats := tcx.DSAdapter.LocalMapStats().NearCacheStats
	inv := tcx.NC.InvalidationRequests() - tcx.decrementInvalidationRequests
	return stats, inv
}

func (tcx *NearCacheTestContext) ResetInvalidationEvents() {
	tcx.decrementInvalidationRequests = tcx.NC.InvalidationRequests()
}

func (tcx *NearCacheTestContext) GetKey(key interface{}) interface{} {
	// TODO: adapt for replicated map
	if tcx.Config.SerializeKeys {
		data, err := tcx.ss.ToData(key)
		if err != nil {
			panic(fmt.Errorf("it: NearCacheTestContext/Get: %w", err))
		}
		return data
	}
	return key
}

func (tcx *NearCacheTestContext) PopulateNearCacheDataAdapter(size int64, valueFmt string) {
	if size < 1 {
		return
	}
	for i := int64(0); i < size; i++ {
		value := fmt.Sprintf(valueFmt, i)
		if _, err := tcx.DSAdapter.Put(context.Background(), i, value); err != nil {
			tcx.T.Fatal(err)
		}
	}
	// TODO: adapt for replicated map
	tcx.AssertNearCacheInvalidationRequests(size)
}

func (tcx *NearCacheTestContext) PopulateNearCacheWithGet(size int64, valueFmt string) {
	for i := int64(0); i < size; i++ {
		value, err := tcx.DSAdapter.Get(context.Background(), i)
		if err != nil {
			tcx.T.Fatal(err)
		}
		target := fmt.Sprintf(valueFmt, i)
		if !assert.Equal(tcx.T, target, value) {
			tcx.T.FailNow()
		}
	}
}

func (tcx *NearCacheTestContext) AssertNearCacheInvalidationRequests(invalidationRequests int64) {
	if tcx.Config.InvalidateOnChange() && invalidationRequests > 0 {
		Eventually(tcx.T, func() bool {
			r := tcx.NC.InvalidationRequests()
			tcx.T.Logf("Expected %d received Near Cache invalidations, but found %d", invalidationRequests, r)
			return invalidationRequests == r
		})
		tcx.ResetInvalidationEvents()
	}
}

func (tcx *NearCacheTestContext) RequireNearCacheSize(target int64) {
	size := int64(tcx.NC.Size())
	require.Equal(tcx.T, target, size, "Cache size didn't reach the desired value")
	// ignoring the invalidation requests here.
	st, _ := tcx.Stats()
	require.Equal(tcx.T, target, st.OwnedEntryCount, "Near Cache owned entry count didn't reach the desired value")
}

func (tcx *NearCacheTestContext) AssertNearCacheStats(owned, hits, misses int64) {
	// ignoring the invalidation requests here.
	stats, _ := tcx.Stats()
	assert.Equal(tcx.T, owned, stats.OwnedEntryCount)
	assert.Equal(tcx.T, hits, stats.Hits)
	assert.Equal(tcx.T, misses, stats.Misses)
}

func (tcx *NearCacheTestContext) AssertNearCacheContent(size int64, valueFmt string) {
	for i := int64(0); i < size; i++ {
		key := tcx.GetKey(i)
		value, found, err := tcx.NC.Get(key)
		if err != nil {
			tcx.T.Fatal(err)
		}
		assert.True(tcx.T, found)
		data, ok := value.(serialization.Data)
		if ok {
			value, err = tcx.ss.ToObject(data)
			if err != nil {
				tcx.T.Fatal(err)
			}
		}
		target := fmt.Sprintf(valueFmt, i)
		if !assert.Equal(tcx.T, target, value) {
			tcx.T.FailNow()
		}
		// TODO: assertNearCacheRecord
	}
}
